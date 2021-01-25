# WOPR Data Engineering Project

A project for CS 410/510 Data Engineering which creates a data pipeline for tracking bus gps breadcrumbs.

## Setup

Clone repo into new directory.

Install python requirements with:

`pip3 install -r requirements.txt`

Create confluent config file `librdkafka.config` of the format:

```python
{
    "bootstrap.servers": "{{server}}",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "{{username}}",
    "sasl.password": "{{password}}"
}
```

and fill in server info for your Confluent Kafka service. Place file somewhere accessible.

Update `producer.py` and `consumer.py` to reflect the location of the config file.

Update `producer.py` and `consumer.py` with correct topic name.

### Cron job to download breadcrumb json data

To set a cron job to run the data gathering script at a particular time each day, open the list of cron jobs with `crontab -e` and follow the instructions in the crontab comments.

As an example, adding the following line will download the data at 12:30 PM every day. Your Python interpreter may not be located at `/usr/bin/python3`, so change that part if necessary:

```30 12 * * * /usr/bin/python3 /path/to/download_json.py``` 

### Systemd service configuration

Add system services in `/etc/systemd/system/` to run `producer.py` and `consumer.py`.

A minimal setup using `systemd` to run `producer.py` using the interpreter `/usr/bin/python3` might look like this:

```[Unit]
Description = Produce daemon
After = network.target 

[Service]
Type = simple
User= {{username}}
ExecStart = /usr/bin/python3 /path/to/producer.py

[Install]
WantedBy = multi-user.target
