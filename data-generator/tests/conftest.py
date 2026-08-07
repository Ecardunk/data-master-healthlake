import sys
from pathlib import Path


GENERATOR_ROOT = Path(__file__).resolve().parents[1]

if str(GENERATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(GENERATOR_ROOT))
