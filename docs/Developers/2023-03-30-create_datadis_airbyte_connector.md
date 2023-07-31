# Create a datadis API airbyte connector

Clone the airbyte repository

```bash
git@github.com:airbytehq/airbyte.git
```

We will follow roughly [The Exchange Rates connector tutorial](https://docs.airbyte.com/connector-development/tutorials/cdk-tutorial-python-http/creating-the-source) and the [cdk python](https://docs.airbyte.com/connector-development/cdk-python/) info.

The cookiecutter-ish connector dev template is started like so:

```bash
cd airbyte-integrations/connector-templates/generator
./generate.sh
```

It will ask for a connector type (ptyhon http api in our case) and a name. `npm` must be available.

airbyte connector acceptance tests require >=python3.9

## Run the source using python

As a script

```bash
# from airbyte-integrations/connectors/source-<name>
python main.py spec
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

## Run the source using docker

As airbyte would do it

```bash
# First build the container
docker build . -t airbyte/source-<name>:dev

# Then use the following commands to run it
docker run --rm airbyte/source-<name>:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-<name>:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-<name>:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-<name>:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```

Then proceed to edit the `airbyte-integrations/connectors/source-datadis/source_datadis/spec.yaml` with the fields you'd want to show in the config. In our case, username and password.

We also added the pypi datadis dependency in the setup.py of our connector.



## OK back from the cave

run with `python main.py read --config secrets/config.json --catalog sample_files/configured_catalog.json`

First the `spec.yaml` is the configuration page, and what will be available with `config['']` in the source.py, nothing more, nothing less. It is filled with the `secrets/config.json`.

What's more rellevant is the `configured_catalog.json` which defines how data records are delivered and the settings page of the source.

You'll need a configured_catalog.json specifying the output schema. If you make it match with the api response we won't need to do any parsing.

```json
{
    "streams": [{
        "stream": {
            "name": "get_consumption_data",
            "source_defined_cursor": false,
            "json_schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "cups": {
                        "type": "string"
                    },
                    "date": {
                        "type": "string",
                        "format": "date"
                    },
                    "time": {
                        "type": "string",
                        "airbyte_type": "time_without_timezone"
                    },
                    "consumptionKWh": {
                        "type": "number"
                    },
                    "obtainMethod": {
                        "type": "string"
                    }
                }
            },
            "supported_sync_modes": ["full_refresh"]
        },
        "sync_mode": "full_refresh",
        "destination_sync_mode": "append"
    }]
}
```

The params are set in the source settings, which is inconvenient because it means creating N sources per params settings. So, you'd be wondering, but startDate will change! The airbyte way of handling querying historical data is configuring [an incremental read](https://docs.airbyte.com/connector-development/config-based/tutorial/incremental-reads/). That way the params would change accordingly.

Also, we will need to query many CUPS. Maybe datadis allows an array as cups param, it does refer to them in plural. Another option might be asyncio-ing the requests but airbyte doesn't seem to be thought like that. One source/connection per CUPS at UI level seems a bad idea. Another option is programatically call/create the airbyte tasks, given a bunch of secrets.

Airbyte facilitates pagination and other goodies, but datadis doesn't offer pagination on the non-agregated data :/

source.py
```python
class DatadisStream(HttpStream, ABC):
   # TODO: Fill in the url base. Required.
    url_base = "https://datadis.es/api-private/api/"

    max_retries = 0

    # default strategy reads one record and then the rest, but datadis doesn't allow repeated requests with the same params :unamused:
    availability_strategy = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.username = config['username']
        self.password = config['password']
        self.cups = config['cups']
        self.token = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "get-consumption-data"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        if not self.token:
            username = self.username
            password = self.password
            self.token = asyncio.run(get_token(username, password))
            print('refreshed token')

        return {"Authorization": "Bearer "+self.token}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.

        We hardcoded the startDate-endDate but it would have to be set in the secrets/config.json and incrementaled instead of full-refresh as this is.
        """
        return {'cups': self.cups, 'distributorCode':2, 'measurementType':0, 'pointType':5, 'startDate': '2022/11', 'endDate': '2023/03', }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Override this method to define how a response is parsed.
        :return an iterable containing each record in the response

        if the json matches the catalog, you don't need any tranformation. If datadis supported pagination you could yield results I guess?
        """

        return response.json()

```

If everything goes well you'll get something like this:

```json
{"type": "LOG", "log": {"level": "INFO", "message": "Starting syncing SourceDatadis"}}
{"type": "LOG", "log": {"level": "INFO", "message": "Syncing stream: get_consumption_data "}}
refreshed token
Requested!
Request: /api-private/api/get-consumption-data?cups=XXX&distributorCode=2&measurementType=0&pointType=5&startDate=2022%2F11&endDate=2023%2F03
{"type": "RECORD", "record": {"stream": "get_consumption_data", "data": {"cups": "XXXX", "date": "2023/03/01", "time": "01:00", "consumptionKWh": 0.076, "obtainMethod": "Real"}, "emitted_at": 1680281671795}}
[...]
{"type": "RECORD", "record": {"stream": "get_consumption_data", "data": {"cups": "XXX", "date": "2023/03/30", "time": "22:00", "consumptionKWh": 0.0, "obtainMethod": "Real"}, "emitted_at": 1680281672153}}
{"type": "RECORD", "record": {"stream": "get_consumption_data", "data": {"cups": "XXX", "date": "2023/03/30", "time": "23:00", "consumptionKWh": 0.0, "obtainMethod": "Real"}, "emitted_at": 1680281672154}}
{"type": "RECORD", "record": {"stream": "get_consumption_data", "data": {"cups": "XXX", "date": "2023/03/30", "time": "24:00", "consumptionKWh": 0.0, "obtainMethod": "Real"}, "emitted_at": 1680281672154}}
{"type": "LOG", "log": {"level": "INFO", "message": "Read 719 records from get_consumption_data stream"}}
{"type": "LOG", "log": {"level": "INFO", "message": "Finished syncing get_consumption_data"}}
{"type": "LOG", "log": {"level": "INFO", "message": "SourceDatadis runtimes:\nSyncing stream get_consumption_data 0:00:05.670986"}}
{"type": "LOG", "log": {"level": "INFO", "message": "Finished syncing SourceDatadis"}}
```

The records are correctly read and parsed, then, theoretically we should implement another connector for the write or rather use a postgres connector and save this stream to a table with a few clicks.

We would probably parse the date-time to datetime and drop the obtainMethod or apply more transforms.

Then any other type of source could just implement their own source connector and then plug to our cedata-api destination connector, which we would have to implement, but then would be decoupled from any source imaginable.

gotchas: datadis api doesn't like repeated requests so the availability and retries have to be disabled.

Here we didn't test it using the docker call. Also, to get this to the official airbyte the whole PR- accept flow needs to be followed. On self-hosted, the soruces api url is configurable and could point to a personal github fork of airbyte and work.