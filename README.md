# Airbyte Destination CSV with Go

Airbyte destination connector made with Go to store data in CSV files.

**NOTE:** CSV files can only be stored in local.

## Usage

From the root of the project, run:

```sh
sh build.sh
```

The above command will build an image ready to use the connector. Be aware of the printed tag.

<div style="width: 500px;">

![Example of destination build](destination_build.png)

</div>

After this, the connector is ready to be used in Airbyte.
