import csv
import datetime
import paho.mqtt.client as mqtt


# Callback function on established MQTT Connection
def on_connect(client, userdata, flags, rc):
    # show in console
    print("Connected with result code " + str(rc))
    # Subscribe my topic
    client.subscribe("haniham/Raw/#")


# Callback function when a MQTT message was received
def on_message(client, userdata, msg):
    # Collect data to write to the CSV
    writeable: list = [datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), msg.topic[-3:], str(msg.payload)]
    # Write the CSV row
    csvwriter.writerow(writeable)
    # Force the file to directly write the line
    # TODO in future versions, reduce forcing a write action to reduce wear on SSD and increase performance
    csvfile.flush()
    # show in console
    print('Writing ', writeable, ' to csv')

# Main function
if __name__ == '__main__':
    # Create new MQTT client
    client = mqtt.Client()
    # Set callback functions
    client.on_connect = on_connect
    client.on_message = on_message
    # Connect to MQTT Broker
    client.connect("broker.mqtt-dashboard.com", 1883, 60)
    # Open file to store the data
    csvfile = open(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.csv'), 'w')
    # Create an CSV writer instance with the opened file
    csvwriter = csv.writer(csvfile, lineterminator='\n')
    # Write the first CSV row
    csvwriter.writerow(['Time', 'Parameter', 'Data'])
    # Force a write
    csvfile.flush()
    # Start the MQTT forever loop
    client.loop_forever()
