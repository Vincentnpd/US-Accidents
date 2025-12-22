"""
SETUP - Create Project Structure
=================================
Run once before first pipeline execution.
"""

from pathlib import Path


def create_directories():
    """Create all necessary directories."""
    
    directories = [
        'data/raw',
        'data/processed/cleaned',
        'data/processed/dimensions',
        'data/processed/fact',
        'data/processed/aggregates',
        'src'
    ]
    
    print("Creating project directories...")
    
    for directory in directories:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        print(f"  Created: {directory}")
    
    print("\nDirectory structure created successfully")
    print("\nNext steps:")
    print("1. Download dataset from Kaggle:")
    print("   https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents")
    print("2. Place US_Accidents_March23.csv in data/raw/")
    print("3. Run: python main.py")


if __name__ == "__main__":
    create_directories()