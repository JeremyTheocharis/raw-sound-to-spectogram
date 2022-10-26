# This program subscribes to the kafka topic ia.raw.audio on the broker united-manufacturing-hub-kafka:9092 using the consumer_group benthos_spectogram_input
# It then takes the data from the topic and converts it to a spectogram and publishes it to the kafka topic ia.raw.spectogram on the broker united-manufacturing-hub-kafka:9092 using the client_id benthos_spectogram_output

# The data is expected to be in binary two channel format 16bit 48kHz
# The output are 1 spectogram for the first channel created using scipy


import pandas as pd
import numpy as np
import json
import sys
from scipy import signal
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from kafka import KafkaConsumer, KafkaProducer
import io
import time

RATE = 48000
IMAGE_SIZE_INCHES = int(5)
DPI = int(60)

def parse_audio_signal(in_data, channels):
    """
    Convert a byte stream into a 2D numpy array with
    shape (chunk_size, channels)
    Samples are interleaved, so for a stereo stream with left channel
    of [L0, L1, L2, ...] and right channel of [R0, R1, R2, ...], the output
    is ordered as [L0, R0, L1, R1, ...]
    Args:
        in_data: the numpy array got from the audio signal
        channels: the number of channels
    Returns:
        A numpy array. For left channel, use result[:, 0], for right channel, use result[:, 1].
    """
    chunk_length = int(len(in_data) / channels)
    # if this is violated the data is malformed and can not be split up into 2 channels properly
    assert chunk_length == int(chunk_length)

    result = in_data.reshape(chunk_length, channels)
    return result

def get_spectogram(snd_block):
    # split the signal into two channels
    split_signal = parse_audio_signal(snd_block, 2)

    # first channel
    f, t, Sxx = signal.spectrogram(split_signal[:, 0], RATE)

    if Sxx.min() == 0:
        masked_array = Sxx[np.nonzero(Sxx)]
        zmin = masked_array.min()
    else:
        zmin = Sxx.min()

    zmax = Sxx.max()

    figure = plt.gcf()
    figure.set_size_inches(IMAGE_SIZE_INCHES, IMAGE_SIZE_INCHES)
    plt.pcolormesh(t, f, Sxx, cmap="RdBu", norm=LogNorm(vmin=zmin, vmax=zmax), shading="auto")
    plt.axis("off")

    # return image as png
    return figure

def main():
    print("Starting spectogram creation")
    # subscribe to kafka topic
    consumer = KafkaConsumer('ia.raw.audio', bootstrap_servers=['united-manufacturing-hub-kafka:9092'], group_id='benthos_spectogram_input')
    # publish to kafka topic
    producer = KafkaProducer(bootstrap_servers=['united-manufacturing-hub-kafka:9092'], client_id='benthos_spectogram_output')

    # create buffer
    buffer = io.BytesIO()

    # endless loop reading from kafka topic
    for msg in consumer:
        # print("Received message")
        # convert the data to a numpy array
        snd_block = np.frombuffer(msg.value, dtype=np.int16)

        # get the spectogram
        spectogram = get_spectogram(snd_block)

        # convert the spectogram to a png, save it in a buffer and publish it to kafka

        # go to the beginning of the buffer
        buffer.seek(0)

        tick = time.perf_counter()
        spectogram.savefig(buffer, format="jpg", dpi=DPI)
        tock = time.perf_counter()
        print(f"savefig {tock - tick:0.4f} seconds")

        buffer.seek(0)

        # publish the png to the kafka topic
        producer.send('ia.raw.spectogram', value=buffer.getvalue())

        buffer.flush()
        buffer.seek(0)

    # close the buffer
    buffer.close()




# boilerplate code for main function
if __name__ == "__main__":
    main()
