from anki import hooks
from aqt import gui_hooks
from aqt import mw
from anki.cards import Card
from aqt.qt import *
from aqt.utils import showInfo, qconnect
from anki.hooks import addHook, wrap

from PyQt5 import QtCore, QtGui, QtWidgets
from .gui import gui_man

def notion_upload():
    pass

def notion_download():
    pass

addHook('profileLoaded', gui_man.load_menu)
addHook('unloadProfile', gui_man.unload_menus)