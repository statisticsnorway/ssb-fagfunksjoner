import logging
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)
report_contents = StringIO()
logger.addHandler(logging.StreamHandler(report_contents))
