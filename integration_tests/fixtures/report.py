from pytest import CollectReport, StashKey, TestReport

PHASE_REPORT_KEY = StashKey[dict[str, CollectReport]]()


class ReportCollector:
    def __init__(self) -> None:
        self.reports: dict[str, TestReport] = {}

    def collect(self, report: TestReport) -> None:
        """Collect a test report."""
        self.reports[report.when] = report

    @property
    def test_failed(self) -> bool:
        """Whether the test run failed."""
        report = self.reports.get("call")
        if not report:
            return False
        return report.failed
