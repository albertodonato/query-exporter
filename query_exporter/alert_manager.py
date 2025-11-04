"""Alert manager for sending alerts to AlertManager."""

import asyncio
import json
import time
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urljoin

import aiohttp
import structlog

from .db import (
    MetricResult,
)


class AlertState:
    """Track the state of an alert instance."""
    
    def __init__(self, alert_key: str):
        self.alert_key = alert_key
        self.start_time: Optional[datetime] = None
        self.last_active: Optional[datetime] = None
        self.active = False
        self.sent = False
        
    def update(self, active: bool, current_time: datetime) -> None:
        """Update alert state."""
        self.last_active = current_time
        
        if active and not self.active:
            # Becoming active
            self.start_time = current_time
            self.active = True
            self.sent = False
        elif not active and self.active:
            # Becoming inactive
            self.active = False
            self.start_time = None
            self.sent = False
        elif active and self.active:
            # Remaining active
            pass


class AlertManager:
    """Client for AlertManager API."""

    def __init__(
        self, 
        url: str, 
        logger: Optional[structlog.stdlib.BoundLogger] = None
    ):
        self.url = url.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logger or structlog.get_logger()
        self._timeout = aiohttp.ClientTimeout(total=30)

    async def start(self) -> None:
        """Initialize the HTTP session."""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=self._timeout)

    async def stop(self) -> None:
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def send_alerts(self, alerts: List[Dict[str, Any]]) -> bool:
        """Send alerts to AlertManager."""
        if not self.url:
            self.logger.debug("AlertManager URL not configured, skipping alert sending")
            return True

        if not self.session:
            await self.start()

        url = urljoin(self.url, '/api/v2/alerts')
        
        try:
            async with self.session.post(
                url, 
                json=alerts,
                headers={'Content-Type': 'application/json'}
            ) as response:
                if response.status == 200:
                    self.logger.debug(
                        "Alerts sent successfully", 
                        count=len(alerts),
                        alert_names=[alert['labels'].get('alertname') for alert in alerts]
                    )
                    return True
                else:
                    self.logger.error(
                        "Failed to send alerts", 
                        status=response.status,
                        response=await response.text()
                    )
                    return False
        except Exception as e:
            self.logger.error("Error sending alerts to AlertManager", error=str(e))
            return False


class AlertGenerator:
    """Generate alerts from query results with condition evaluation and duration tracking."""

    def __init__(
        self, 
        alert_manager: AlertManager, 
        alert_configs: Dict[str, Any],
        logger: Optional[structlog.stdlib.BoundLogger] = None
    ):
        self.alert_manager = alert_manager
        self.alert_configs = alert_configs
        self.logger = logger or structlog.get_logger()
        
        # Track alert states: {alert_key: AlertState}
        self.alert_states: Dict[str, AlertState] = {}
        
        # Cache for parsed conditions: {condition_string: (operator, threshold)}
        self._condition_cache: Dict[str, Tuple[str, float]] = {}

    def generate_alerts_from_results(
        self, 
        query_name: str, 
        alert_names: List[str], 
        results: List[Dict[str, Any]],  # 这里已经是字典列表
        database_labels: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Generate alerts from query results with condition evaluation."""
        alerts = []
        current_time = datetime.utcnow()
        
        for alert_name in alert_names:
            alert_config = self.alert_configs.get(alert_name)
            if not alert_config:
                self.logger.warning(
                    "Alert configuration not found", 
                    alert_name=alert_name,
                    query=query_name
                )
                continue

            for result in results:
                # Check if alert condition is met
                is_active = self._evaluate_alert_condition(alert_config, result)
                
                # Create unique key for this alert instance
                alert_key = self._create_alert_key(alert_name, result, database_labels)
                
                # Update alert state
                alert_state = self._update_alert_state(alert_key, is_active, current_time)
                
                # Check if alert should be sent based on duration
                should_send = self._should_send_alert(alert_config, alert_state, current_time)
                
                if should_send and not alert_state.sent:
                    alert = self._create_alert(
                        alert_name, 
                        alert_config, 
                        result,  # 这里传递的是字典
                        database_labels,
                        query_name,
                        alert_state
                    )
                    if alert:
                        alerts.append(alert)
                        alert_state.sent = True
                        self.logger.debug(
                            "Alert triggered",
                            alert_name=alert_name,
                            query=query_name,
                            duration=self._get_duration_seconds(alert_state.start_time, current_time),
                            condition_met=is_active
                        )
        
        return alerts

    def _evaluate_alert_condition(self, alert_config: Dict[str, Any], result: Dict[str, Any]) -> bool:
        """Evaluate if alert condition is met based on result value."""
        try:
            if 'value' not in result:
                self.logger.debug(
                    "Result missing 'value' field, skipping condition evaluation",
                    result_keys=list(result.keys())
                )
                return False
                
            value = result['value']
            
            # Handle None values
            if value is None:
                self.logger.debug("Result value is None, skipping condition evaluation")
                return False
            
            # Convert to number for comparison
            numeric_value = self._convert_to_numeric(value)
            if numeric_value is None:
                self.logger.debug(
                    "Cannot convert result value to number, skipping condition evaluation",
                    value=value,
                    value_type=type(value).__name__
                )
                return False
            
            # Parse condition from alert config
            condition = alert_config.get('condition', '> 0')
            return self._evaluate_condition(numeric_value, condition)
            
        except Exception as e:
            self.logger.error(
                "Failed to evaluate alert condition",
                error=str(e),
                alert_config=alert_config,
                result=result
            )
            return False

    def _convert_to_numeric(self, value: Any) -> Optional[float]:
        """Convert value to numeric type for comparison."""
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            try:
                # Remove any whitespace and try to convert
                cleaned_value = value.strip()
                return float(cleaned_value)
            except (ValueError, TypeError):
                return None
        
        # Try generic conversion
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _evaluate_condition(self, value: float, condition: str) -> bool:
        """Evaluate a condition string against a value using cached parsing."""
        try:
            # Use cached parsed condition or parse and cache it
            if condition not in self._condition_cache:
                parsed_condition = self._parse_condition(condition)
                if parsed_condition is None:
                    self.logger.warning(
                        "Invalid condition format, using default '> 0'",
                        condition=condition
                    )
                    parsed_condition = ('>', 0.0)
                self._condition_cache[condition] = parsed_condition
            
            operator, threshold = self._condition_cache[condition]
            
            # Evaluate the condition
            if operator == '>':
                return value > threshold
            elif operator == '>=':
                return value >= threshold
            elif operator == '<':
                return value < threshold
            elif operator == '<=':
                return value <= threshold
            elif operator == '==':
                return value == threshold
            elif operator == '!=':
                return value != threshold
            else:
                self.logger.error("Unknown operator in condition", operator=operator)
                return False
                
        except Exception as e:
            self.logger.error(
                "Failed to evaluate condition",
                condition=condition,
                value=value,
                error=str(e)
            )
            return False

    def _parse_condition(self, condition: str) -> Optional[Tuple[str, float]]:
        """Parse condition string into operator and threshold."""
        try:
            # Normalize the condition string
            condition = condition.strip()
            
            # Use regex to parse the condition
            pattern = r'^\s*(>|>=|<|<=|==|!=)\s*([+-]?\d*\.?\d+)\s*$'
            match = re.match(pattern, condition)
            
            if not match:
                self.logger.warning("Invalid condition format", condition=condition)
                return None
            
            operator, value_str = match.groups()
            threshold = float(value_str)
            
            return (operator, threshold)
            
        except (ValueError, TypeError) as e:
            self.logger.error(
                "Failed to parse condition",
                condition=condition,
                error=str(e)
            )
            return None

    
    def _create_alert_key(self, alert_name: str, result_labels: Dict[str, Any], database_labels: Dict[str, str]) -> str:
        """Create a unique key for an alert instance."""
        # Use labels to create unique key for this alert instance
        label_parts = []
        
        # Add database labels
        for key, value in sorted(database_labels.items()):
            label_parts.append(f"{key}:{value}")
            
        # Add result labels
        for key, value in sorted(result_labels.items()):
            # 跳过指标值字段
            if key == 'value' or (isinstance(value, (int, float)) and key not in ['xxxx', 'yyyy']):
                continue
            if isinstance(value, (str, int, float)):
                label_parts.append(f"{key}:{value}")
                
        return f"{alert_name}:{':'.join(label_parts)}"
    
    def _update_alert_state(self, alert_key: str, is_active: bool, current_time: datetime) -> AlertState:
        """Update or create alert state."""
        if alert_key not in self.alert_states:
            self.alert_states[alert_key] = AlertState(alert_key)
            
        alert_state = self.alert_states[alert_key]
        alert_state.update(is_active, current_time)
        
        return alert_state

    def _should_send_alert(self, alert_config: Dict[str, Any], alert_state: AlertState, current_time: datetime) -> bool:
        """Check if alert should be sent based on duration."""
        if not alert_state.active or alert_state.start_time is None:
            return False
            
        # Parse duration from alert config (e.g., "10m", "1h", "30s")
        duration_str = alert_config.get('for', '0m')
        required_duration = self._parse_duration(duration_str)
        
        actual_duration = self._get_duration_seconds(alert_state.start_time, current_time)
        
        return actual_duration >= required_duration

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds."""
        try:
            duration_str = duration_str.strip().lower()
            
            if duration_str.endswith('s'):
                return int(duration_str[:-1])
            elif duration_str.endswith('m'):
                return int(duration_str[:-1]) * 60
            elif duration_str.endswith('h'):
                return int(duration_str[:-1]) * 3600
            elif duration_str.endswith('d'):
                return int(duration_str[:-1]) * 86400
            else:
                # Assume minutes if no unit specified
                return int(duration_str) * 60
                
        except (ValueError, TypeError):
            self.logger.warning("Invalid duration format, using 0", duration=duration_str)
            return 0

    def _get_duration_seconds(self, start: datetime, end: datetime) -> float:
        """Get duration in seconds between two datetimes."""
        return (end - start).total_seconds()

    def _create_alert(
        self, 
        alert_name: str, 
        alert_config: Dict[str, Any], 
        result: Dict[str, Any],
        database_labels: Dict[str, str],
        query_name: str,
        alert_state: AlertState
    ) -> Optional[Dict[str, Any]]:
        """Create a single alert from result data."""
        try:
            self.logger.debug(
                "Creating alert",
                alert_name=alert_name,
                result=result,
                alert_config_labels=alert_config.get('labels'),
                alert_config_labels_type=type(alert_config.get('labels')).__name__
            )
            
            # 合并标签：数据库标签 + 告警配置标签 + 查询结果标签
            labels = database_labels.copy()
            self.logger.debug("database_labels",labels=labels)
            print(f"database_labels: {labels}")
            
            # 正确处理 alert_config 中的 labels
            alert_config_labels = alert_config.get('labels', {})
            if isinstance(alert_config_labels, list):
                # 如果 labels 是列表，将其转换为字典，从查询结果中获取对应的值
                labels_dict = {}
                for label_key in alert_config_labels:
                    if label_key in result:
                        labels_dict[label_key] = str(result[label_key])
                labels.update(labels_dict)
            elif isinstance(alert_config_labels, dict):
                # 如果 labels 已经是字典，直接使用
                labels.update(alert_config_labels)
            
            # 从查询结果中提取其他标签字段
            for key, value in result.items():
                # 跳过指标值字段和已经处理过的标签字段
                if key == 'value' or key == 'metric' or key in labels:
                    continue
                # 只处理字符串、数字类型的值作为标签
                if isinstance(value, (str, int, float)):
                    labels[key] = str(value)
            
            # 设置必需的标签
            labels['alertname'] = alert_name
            labels['severity'] = alert_config.get('severity', 'warning')
            labels['query'] = query_name

            # 构建注解
            annotations = alert_config.get('annotations', {}).copy()
            if 'summary' not in annotations:
                annotations['summary'] = alert_config.get('summary', alert_name)
            if 'description' not in annotations:
                annotations['description'] = alert_config.get('description', '')
            
            # 获取指标值
            value = result.get('value')
            annotations['value'] = str(value) if value is not None else 'unknown'
            
            # Add duration information
            if alert_state.start_time:
                duration_seconds = self._get_duration_seconds(alert_state.start_time, datetime.utcnow())
                annotations['duration'] = f"{duration_seconds:.0f}s"

            alert = {
                'labels': labels,
                'annotations': annotations,
                'startsAt': alert_state.start_time.isoformat() + 'Z' if alert_state.start_time else datetime.utcnow().isoformat() + 'Z',
                'generatorURL': f'http://query-exporter/alerts?query={query_name}'
            }

            self.logger.debug(
                "Alert created successfully",
                alert_name=alert_name,
                labels=labels,
                annotations=annotations
            )
            
            return alert

        except Exception as e:
            self.logger.error(
                "Failed to create alert", 
                alert_name=alert_name,
                error=str(e),
                result=result,
                alert_config=alert_config,
                traceback=True  # 这会显示完整的堆栈跟踪
            )
            return None
    
    def cleanup_expired_states(self, max_age_seconds: int = 3600) -> None:
        """Clean up expired alert states to prevent memory leaks."""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, state in self.alert_states.items():
            if state.last_active and self._get_duration_seconds(state.last_active, current_time) > max_age_seconds:
                expired_keys.append(key)
                
        for key in expired_keys:
            del self.alert_states[key]
            
        if expired_keys:
            self.logger.debug("Cleaned up expired alert states", count=len(expired_keys))