import sys
from pathlib import Path

# The fake HubSpot (tests/fake_hubspot.py) is imported by name.
sys.path.insert(0, str(Path(__file__).parent))
