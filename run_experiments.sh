mkdir build
chmod 777 build
cd ./build
cmake -DCMAKE_BUILD_TYPE=Release ..
make

# Run experiments
./kv-experiment

# Plot graphs
python3 ../plot_generator.py

