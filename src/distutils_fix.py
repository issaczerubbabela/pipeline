# Fix for distutils deprecation in Python 3.12+
import sys

# Add setuptools as fallback for distutils
try:
    import distutils
except ImportError:
    try:
        import setuptools
        sys.modules['distutils'] = setuptools
    except ImportError:
        pass

# Alternative fix using packaging if available
try:
    from packaging import version
except ImportError:
    pass
