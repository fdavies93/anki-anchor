[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/fd93)
[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC_BY--NC--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

# ⚓ Anchor: Connect Anki to The World

### To Compile .ui Files

The UI files are from QT Creator and are easiest to edit using that software.

Once you've edited them, you can convert them into Python code using the pyuic command in the main folder:
``` pyuic5 ./qt_ui/FILENAME.ui -o ./FILENAME.py ```

### External Libraries

Anchor does not require installing any additional libraries or software.

Although using Pandas or another data handling library would have simplified development of this addon, it also would have required end users to install several additional software libraries, and was therefore avoided in favour of a bespoke data-manipulation implementation (the DataSet class).

### How Synchronisation Works

Synchronisation between different data storage mediums can be challenging, so Anchor tries to normalise the process as far as possible. Whether downloading data from an external service (for example, Notion) to Anki or uploading from Anki to another service, the process is the same.

In an upload, the data source is Anki and the destination is the external service. In a download, the data source is the external service, and the destination is Anki.

1. Data from the source and destination is read into a DataSet object with as few changes to the data as possible. For example, all Anki fields other than tags are transformed into strings by stripping HTML information; tags is read into a list of strings (MULTISELECT data type).
2. Using mapping information, the source dataset is transformed to match the destination dataset. This is done by changing the names and datatypes of source columns until they match the destination columns. Where individual rows of data can't be converted, users will be warned that some data might be lost in the synchronisation process. Where column types are incompatible (for example, multiselect to date), the mapping will be considered invalid and synchronisation will not happen.
3. Data is written to the destination, again changing the data as little as possible in the process.