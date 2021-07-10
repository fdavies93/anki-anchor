# anki2notion Plugin

### To Compile .ui Files

The UI files are from QT Creator and are easiest to edit using that software.

Once you've edited them, you can convert them into Python code using the pyuic command in the main folder:
``` pyuic5 ./qt_ui/FILENAME.ui -o ./FILENAME.py ```