# -*- coding: utf-8 -*-
from MysqlUpgrade import MysqlUpgradeTool
import os

if __name__ == '__main__':
    mysql_uprage = MysqlUpgradeTool()
    mysql_uprage.upgrade()
    os.system("Pause")
