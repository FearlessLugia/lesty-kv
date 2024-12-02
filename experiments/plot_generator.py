import matplotlib.pyplot as plt
import pandas as pd


def read_csv_and_plot(file_path, file_name, title):
    # Read data from CSV file
    data = pd.read_csv(file_path)

    plt.figure(figsize=(8, 8))
    plt.plot(
        data['Data Size'],
        data[f'{title} Throughput' if title != 'Get' else 'Binary Search Throughput'],
        marker='o',
        linestyle='-',
        linewidth=2.5,
        label=f'{title} Throughput')

    # Set log scale for x axis
    plt.xscale('log')

    ticks = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    plt.xticks(ticks, labels=[str(tick) for tick in ticks])

    plt.title(f'{title} Throughput vs Data Size (Log Scale)', fontsize=14)
    plt.xlabel('Data Size (log scale)', fontsize=12)
    plt.ylabel(f'{title} Throughput', fontsize=12)

    plt.tight_layout()
    plt.savefig(file_name)
    plt.show()


read_csv_and_plot('experiment3_Put.csv', 'put_plot.png', 'Put')
read_csv_and_plot('experiment3_Get.csv', 'get_plot.png', 'Get')
read_csv_and_plot('experiment3_Scan.csv', 'scan_plot.png', 'Scan')
