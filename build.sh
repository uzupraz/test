python -m venv code/venv
source code/venv/bin/activate
pip install -r code/requirements.txt

# Run Python tests
cd code
python run_tests.py
cd ..

exit_code=$?

if [ $exit_code -ne 0 ]; then
    echo "Test failed!"
    exit 1
fi

#ZIP all required files
cd code/venv/lib/python3.11/site-packages/
zip -r9 ${OLDPWD}/dist.zip .
cd ${OLDPWD}

# Remove venv after packaging all dependencies
rm -rf code/venv
cd code
zip -r ../dist.zip .
