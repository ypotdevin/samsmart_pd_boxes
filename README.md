# SamSmart Presence Detection Boxes

A library to pre-process, analyze and plot sensor data, captured by the two
presence detection (PD) boxes of the SamSmart project, hosted at open.INC.

## The open.INC Data Web API

The sensor data is accessible through the [open.INC Data Web
API](https://github.com/open-inc/openware/wiki/Data-Web-API). The main exchange
format of that API is JSON. Since JSON is not an ideal format for data science,
this library provides functions to convert JSON into
[`pd.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html)
and
[`pd.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html)
for further processing and downstream tasks.

## How To Use This Library

### Configuration

First, make sure you are able to access the non-public open.INC Data Web API by
obtaining a valid OD-SESSION header. Once you have one of those, duplicate the
`config/example_config.toml` file, rename it to just `config.toml` and update
the assignment `od_session = "..."` with your obtained header. In case the base
URL has changed since writing this how to, update it too.

### Resources

There are two presence detection boxes, placed in different SamSmart households
for testing. There are 15–20 of such SamSmart households that participate in the
presence detection experiment, which is why, over time, the PD boxes move from
one household to another. In the `resources/households.toml` file, the
households are identified by strings like `"haushaltXX"`, where `XX` are digits
(e.g. `"haushalt06"`). Those identifiers are closely related – but not identical
– to the identifier open.INC uses in their database (tags). Those tags have,
usually, the shape `"sshX"` or `"sshXX"` where X is a digit.

As long as the PD boxes are in operation and even change their location, you
have to update the `resources/households.toml` file accordingly, to make use of
the most recent data. To do so, create new sections that describe at which times
which box ("koffer") is located in which household. One caveat is that the
open.INC database is not always synchronous to the box movement. When a box
moves from the previous household to the next household, it takes manual
intervention by open.INC to change the open.INC tag, which is associated with
the new recording. Therefore, for a couple of hours/days, the sensor data
captured at the next household, is identified still by the previous tag. Only
after the manual intervention, the tag aligns again with the proper household
ID. This is the reason why you find entries in `resources/households.toml`,
where the tag does not match the household identifier (these are exactly the
above mentioned transition periods).

A minor note: The most recent entry naturally won't have a `newest_record` date.
Just leave it empty, until you know a final date and time (which is usually the
case when the box moves to the next location). The library will automatically
fill in the missing datetime by the current datetime.

Additional to the book keeping of which box is where at what time,
`resources/households.toml` also contains a section about the available sensors
(their identifiers) and the kind of their signal (nominal/boolean or
cardinal/float).

Both sections may be conveniently accessed through the constants
`AVAILABLE_SENSORS` and `HOUSEHOLDS` (defined in `resources/__init__.py` but
available at top level module import).

### Loading Data

Use

### Feature Engineering

TODO

### Plotting

TODO
