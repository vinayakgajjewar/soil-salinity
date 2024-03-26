# soil-salinity

This project combines California farmland vector data and satellite soil salinity data and displays the result in an interactive web interface.

## Features

- Aggregation function selection (minimum, maximum, average, standard deviation)
- Raster data selection
- Interactive farmland data front-end interface
- Dynamic extents

## Installation

### Dependencies

The soil salinity backend relies upon Java 1.8.0 and Scala 2.12.7.

### Setup

This project expects all data files (shapefile, GeoTIFF) to be stored in the `data/` directory.
The data directory should be organized as follows:

![data directory](doc/images/directory_organization.png)

### Run in development
To run the server in development mode, run the class "`edu.ucr.cs.bdlab.beast.operations.Main`" with command line
argument `server`.

### Server deployment
Place the `data/` on the server at which you want it to be hosted.
Install Beast CLI and run the following command at the same directory where you have 
the `data` directory (not inside the `data` directory).

```shell
beast --jars futurefarmnow-backend-*.jar server
```

In the directory where you run `beast server`, you can place a file `beast.properties` to set the default
system parameters, e.g., `port`.

**Configure through Apache:** If you want to make the server accessible through Apache, you can add the following
configuration to your Apache web server.

```
<VirtualHost *:80>
    <Directory /var/www/sites/ffn.cs.ucr.edu/public_html>
        Require all granted
        AllowOverride All
        RewriteEngine On
        RewriteCond %{REQUEST_URI}  ^/futurefarmnow-backend-0.2-SNAPSHOT/(.*)$
        RewriteRule ^futurefarmnow-backend-0.2-SNAPSHOT/(.*)$ http://localhost:8081/$1 [P,L]
        RewriteCond %{REQUEST_URI}  ^/futurefarmnow-backend-[\.0-9]*(-[\w\d]+)?/(.*)$
        RewriteRule ^(.*)$ http://localhost:8080/$1 [P,L]
   </Directory>
</VirtualHost>
```

The first RewriteRule forwards all requests that begin with `/futurefarmnow-backend-0.2-SNAPSHOT/` to the server
running on port 8081. The second rewrite rule forwards requests for any version to the server running on port 8080.
This configuration allows you to deploy a different version of the API while keeping the stable version running.

### API
Check the detailed [API description here](doc/api.md).

### Add vector dataset
Check the [step-by-step instructions for adding a new vector dataset](doc/add-vector-dataset.md).

## License

Copyright 2024 University of California, Riverside

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.