mkdir build
chmod 777 build
cd ./build
cmake -DCMAKE_BUILD_TYPE=Release ..
make

# Run tests
./kv-test