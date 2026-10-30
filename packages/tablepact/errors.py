"""Errors from TablePact's own contracts; native execution errors are separate."""

__all__ = [
    "TablePactError", "SpecError", "WriteError", "DescriptionApplyError",
]


class TablePactError(ValueError):
    """An invalid TablePact contract or input."""


class SpecError(TablePactError):
    """Invalid Table Spec or output declaration."""


class WriteError(TablePactError):
    """Invalid Job overwrite input; native write errors are not wrapped."""


class DescriptionApplyError(RuntimeError):
    """Data write returned, but description updates may be partially applied."""

    def __init__(self, target):
        self.target = target
        self.stage = "description"
        self.data_written = True
        super().__init__(f"Data write completed for {target}; description application failed and may be partial")
