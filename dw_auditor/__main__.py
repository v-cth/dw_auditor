"""
Entry point for dw_auditor CLI
"""
import sys
from pathlib import Path

# Add parent directory to path to import audit module
sys.path.insert(0, str(Path(__file__).parent.parent))

from audit import main

if __name__ == '__main__':
    main()
