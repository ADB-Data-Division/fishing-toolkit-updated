"""Base API for PyWebView communication."""

from datetime import datetime


class BaseApi:
    """Base class for PyWebView API bridges."""

    def __init__(self, window=None):
        """Initialize the API.

        Args:
            window: PyWebView window instance for JavaScript evaluation
        """
        # Store window in a way that won't be serialized by pywebview
        # Using double underscore to make it name-mangled and harder to serialize
        self.__window = window

    @property
    def window(self):
        """Get window reference safely."""
        return self.__window

    @window.setter
    def window(self, value):
        """Set window reference safely."""
        self.__window = value

    def __getstate__(self):
        """Custom serialization to exclude window from being serialized."""
        state = self.__dict__.copy()
        # Remove window from state to prevent serialization issues
        state.pop("_BaseApi__window", None)
        for key in list(state.keys()):
            if "window" in key.lower() and key.startswith("_"):
                state.pop(key, None)
        return state

    def __setstate__(self, state):
        """Custom deserialization."""
        self.__dict__.update(state)
        self.__window = None

    def __dir__(self):
        """Override dir() to hide window from introspection."""
        # Get all attributes but exclude private window attribute
        attrs = [
            attr
            for attr in object.__dir__(self)
            if not (attr.startswith("_") and "window" in attr.lower() and not attr.startswith("__"))
        ]
        return sorted(set(attrs))

    def console_log(self, level: str, message: str):
        """Bridge console logs from frontend to Python.

        Args:
            level: Log level (info, warn, error, debug)
            message: Log message
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] JS-{level.upper()}: {message}")

    def notify_frontend(self, event: str, data: dict | None = None):
        """Send notification to frontend JavaScript.

        Args:
            event: Event name
            data: Optional data to send with the event
        """
        if not self.window:
            return

        try:
            data_json = "null" if data is None else str(data)
            js_code = f"""
                if (window.dashboard && window.dashboard.on{event.capitalize()}) {{
                    window.dashboard.on{event.capitalize()}({data_json});
                }}
            """
            self.window.evaluate_js(js_code)
        except Exception as e:
            print(f"Error notifying frontend: {e}")

    def notify_data_update(self):
        """Notify the frontend that data has been updated."""
        self.notify_frontend("dataUpdate")

    def notify_error(self, error_message: str):
        """Notify the frontend of an error.

        Args:
            error_message: Error message to display
        """
        self.notify_frontend("error", {"message": error_message})

    def close(self):
        """Clean up resources. Override in subclasses."""
        pass
