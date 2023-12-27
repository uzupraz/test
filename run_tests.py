import unittest
from local_runner import app

if __name__=='__main__':
    # Discover tests at path with the file name as a pattern (if any).
    loader = unittest.TestLoader()

    start_dir = 'tests'
    pattern = 'test_*.py'
    top_level_dir = '.'

    args = {  # noqa: F841
        "start_dir": start_dir,
        "pattern": pattern,
        "top_level_dir": top_level_dir,
    }
    suite = loader.discover(start_dir, pattern, top_level_dir)

    runner = unittest.TextTestRunner(
            tb_locals=False,
            failfast=False,
            verbosity=1,
        )
    # lets try to tailer our own suite so we can figure out running only the ones we want
    result = runner.run(suite)  # type: ignore
    if not result.wasSuccessful():
        exit(1)