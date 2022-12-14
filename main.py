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
import base64
import os
import paho.mqtt.client as mqtt
import uuid

RATE = 48000
IMAGE_SIZE_INCHES = int(5)
DPI = int(60)

INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "ia.raw.audio")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "ia.raw.audio.spectogram")
CHANNEL = int(os.environ.get("CHANNEL", "1"))

# read env variable MQTTMode with default value false
MQTTMode = os.environ.get("MQTTMode", "false")


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
    f, t, Sxx = signal.spectrogram(split_signal[:, CHANNEL], RATE)

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

    uid = str(uuid.uuid4())+".jpg"
    tick = time.perf_counter()
    plt.savefig(uid, dpi=DPI)
    plt.close()
    tock = time.perf_counter()
    print(f"savefig {tock - tick:0.4f} seconds for {uid}")

    return uid

# this is the main function if MQTT is used
def main_mqtt():
    print("Starting spectogram creation in MQTT")
    # subscribe to mqtt topic
    client = mqtt.Client()
    client.connect("mqtt_broker", 1883, 60)
    # subscribe to the topic
    client.subscribe(INPUT_TOPIC)

    # foreach message received, create a spectogram and publish it to the output topic
    client.on_message = on_message
    client.loop_forever()

def on_message(client, userdata, msg):
    print("Received message")
    snd_block = np.frombuffer(msg.payload, dtype=np.int16)

    # set timestamp_ms to the current time in milliseconds
    timestamp_ms = int(round(time.time() * 1000))

    # get the spectogram, function returns the UID of the file
    spectogramUID = get_spectogram(snd_block)

    # read the file spectogram.jpg and publish it to kafka
    with open(spectogramUID, "rb") as f:
        image = f.read()
        byteArr = base64.b64encode(image).decode("utf-8")

        print(f"Processing for timestamp {timestamp_ms}")
        prepared_message = {
            "timestamp_ms": str(timestamp_ms),
            "image": {
                "image_bytes": str(byteArr),
            }
        }
        # publish the message to the output topic
        client.publish(OUTPUT_TOPIC, json.dumps(prepared_message))

    # delete the spectogram
    os.remove(spectogramUID)


# this is the main function if Kafka is used (default)
def main_kafka():
    print("Starting spectogram creation")
    # subscribe to kafka topic
    consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=['united-manufacturing-hub-kafka:9092'], group_id='benthos_spectogram_input_'+str(CHANNEL))
    # publish to kafka topic
    producer = KafkaProducer(bootstrap_servers=['united-manufacturing-hub-kafka:9092'], client_id='benthos_spectogram_output'+str(CHANNEL))

    # endless loop reading from kafka topic
    for msg in consumer:
        # print("Received message")
        # convert the data to a numpy array
        snd_block = np.frombuffer(msg.value, dtype=np.int16)

        # get the spectogram
        spectogramUID = get_spectogram(snd_block)

        # read the file spectogram.jpg and publish it to kafka
        with open(spectogramUID, "rb") as f:
            image = f.read()
            byteArr = base64.b64encode(image).decode("utf-8")

            # set timestamp_ms to timestamp of the message (this is added by Kafka to each message, the payload does not contain a timestamp yet)
            timestamp_ms = msg.timestamp
            print(f"Processing for timestamp {timestamp_ms}")
            prepared_message = {
                "timestamp_ms": str(timestamp_ms),
                "imageAsBase64": str(byteArr)
            }
            producer.send(OUTPUT_TOPIC, json.dumps(prepared_message).encode('utf-8'))

        tock = time.perf_counter()
        print(f"producer.send {tock - tick:0.4f} seconds")

        # delete the spectogram
        os.remove(spectogramUID)

# boilerplate code for main function
if __name__ == "__main__":
    if MQTTMode == "true":
        main_mqtt()
    else:
        main_kafka()
