### The script takes the data obtained during the experiment and computes an estimate overhead, considering the total bytes sent and the byte size of the files sent.

# Example of use:
python post_measurement_analysis.py --folder "test_wifi" --outdir "test_wifi"

# where:
    --folder where the exp data was saved.
    --outdir output folder to save a json with the metrics computed. If none, it'll just show it in cmd.
