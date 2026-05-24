from query_exporter.metrics import BuiltinMetrics


class TestBuiltinMetrics:
    def test_names(self) -> None:
        assert BuiltinMetrics.names() == frozenset(
            {
                "database_errors",
                "queries",
                "query_interval",
                "query_latency",
                "query_timestamp",
            }
        )

    def test_get_configs_return_all_builtin_metrics(self) -> None:
        configs = BuiltinMetrics.get_configs(frozenset(), {})
        assert set(configs) == BuiltinMetrics.names()

    def test_get_configs_add_extra_labels(self) -> None:
        configs = BuiltinMetrics.get_configs(frozenset({"database"}), {})
        for name, config in configs.items():
            if name == "query_interval":
                assert "database" not in config.labels
            else:
                assert "database" in config.labels

    def test_get_configs_query_interval_not_affected_by_extra_labels(
        self,
    ) -> None:
        configs_no_extra = BuiltinMetrics.get_configs(frozenset(), {})
        configs_with_extra = BuiltinMetrics.get_configs(
            frozenset({"database"}), {}
        )
        assert (
            configs_no_extra["query_interval"].labels
            == configs_with_extra["query_interval"].labels
        )

    def test_get_configs_builtin_metrics_config_overrides_applied(
        self,
    ) -> None:
        configs = BuiltinMetrics.get_configs(
            frozenset(), {"queries": {"increment": False}}
        )
        assert configs["queries"].config["increment"] is False
