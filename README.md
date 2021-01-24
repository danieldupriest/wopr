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
    "sasl.username": {{username}},
    "sasl.password": "{{password}}"
}
```

and fill in server info for your Confluent Kafka service. Place file somewhere accessible.

Update `producer.py` and `consumer.py` to reflect the location of the config file.

Update `producer.py` and `consumer.py` with correct topic name.

### Cron job to download breadcrumb json data

### Systemd service configuration
