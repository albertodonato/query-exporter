"""Alert manager for sending alerts to AlertManager."""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from urllib.parse import urljoin

import aiohttp
import structlog


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
    """Generate alerts from query results."""

    def __init__(
        self, 
        alert_manager: AlertManager, 
        alert_configs: Dict[str, Any],
        logger: Optional[structlog.stdlib.BoundLogger] = None
    ):
        self.alert_manager = alert_manager
        self.alert_configs = alert_configs
        self.logger = logger or structlog.get_logger()

    def generate_alerts_from_results(
        self, 
        query_name: str, 
        alert_names: List[str], 
        results: List[Dict[str, Any]],
        database_labels: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Generate alerts from query results."""
        alerts = []
        
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
                alert = self._create_alert(
                    alert_name, 
                    alert_config, 
                    result, 
                    database_labels,
                    query_name
                )
                if alert:
                    alerts.append(alert)
        
        return alerts

    def _create_alert(
        self, 
        alert_name: str, 
        alert_config: Dict[str, Any], 
        result: Dict[str, Any],
        database_labels: Dict[str, str],
        query_name: str
    ) -> Optional[Dict[str, Any]]:
        """Create a single alert from result data."""
        try:
            # 合并标签：数据库标签 + 告警配置标签 + 查询结果标签
            labels = database_labels.copy()
            labels.update(alert_config.get('labels', {}))
            
            # 从查询结果中提取标签（排除值字段）
            for key, value in result.items():
                if key != 'value' and isinstance(value, (str, int, float)):
                    labels[str(key)] = str(value)
            
            # 设置必需的标签
            labels['alertname'] = alert_name
            labels['severity'] = alert_config.get('severity', 'warning')
            labels['query'] = query_name

            # 构建注解
            annotations = alert_config.get('annotations', {}).copy()
            annotations['summary'] = alert_config.get('summary', alert_name)
            annotations['description'] = alert_config.get('description', '')
            if 'value' in result:
                annotations['value'] = str(result['value'])

            alert = {
                'labels': labels,
                'annotations': annotations,
                'startsAt': datetime.utcnow().isoformat() + 'Z',
                'generatorURL': f'http://query-exporter/alerts?query={query_name}'
            }

            return alert

        except Exception as e:
            self.logger.error(
                "Failed to create alert", 
                alert_name=alert_name,
                error=str(e)
            )
            return None