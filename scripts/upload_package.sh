export $(cat .env)

python setup.py bdist_wheel
axiom dataset upload --id 188 -v v0.1 -p dist
axiom dataset ls --id 188 -v v0.1

rm -rf build
rm -rf dist
rm -rf dialgent.egg-info

