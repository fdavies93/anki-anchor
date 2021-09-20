from anki import hooks
from aqt import gui_hooks
from aqt import mw
from anki.cards import Card
from aqt.qt import *
from aqt.utils import showInfo, qconnect
from anki.hooks import addHook, wrap

from PyQt5 import QtCore, QtGui, QtWidgets
from .gui import gui
from .model import model

addHook('profileLoaded', gui.load_menu)
addHook('profileLoaded', model.load_config)
addHook('unloadProfile', gui.unload_menus)