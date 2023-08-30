import gc
import cgitb
import os.path
import sys
import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.Qt import Qt
from PyQt5.QtCore import QRunnable, QThreadPool, QTimer
from PyQt5.QtWidgets import QMainWindow, QApplication, QFileDialog, QMessageBox, QDialog, QInputDialog
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pyqtgraph import ViewBox, mkPen, InfiniteLine
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import ui_Monitor_software
import ui_Monitor_software_sub

cgitb.enable(format='text')
plt.switch_backend('agg')


# 主程式
class MainForm(QMainWindow, ModbusClient):
    def __init__(self, parent=None):
        super(MainForm, self).__init__(parent)
        self.time = QTimer(self)
        self.time_2 = QTimer(self)
        self.schedule = BackgroundScheduler({
            'apscheduler.executors.default': {
                'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                'max_workers': '25'},
            'apscheduler.job_defaults.coalesce': 'false',
            'apscheduler.timezone': 'Asia/Taipei'
        })
        config = ConfigParser()
        config.read('./config.ini')
        db_ip = config['DBURL']['db_ip']
        db_port = config['DBURL']['db_port']
        db_url = f"""postgresql+psycopg2://{user}:{pwd}@{db_ip}:{db_port}/"""
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine, autocommit=True)
        self.session = Session()
        self.ui = ui_Monitor_software.Ui_MainWindow()
        self.ui.setupUi(self)
        self.initUI()

    def initUI(self):  # 初始化程式參數
        # page 帳號資訊 - 按鈕連結設定
        self.ui.pushButton.clicked.connect(self.login)
        self.ui.lineEdit.returnPressed.connect(self.ui.pushButton.click)
        self.ui.lineEdit_2.returnPressed.connect(self.ui.pushButton.click)
        self.ui.pushButton_2.clicked.connect(self.logout)
        self.ui.pushButton_3.clicked.connect(self.add_account)
        self.ui.lineEdit_5.returnPressed.connect(self.ui.pushButton_3.click)
        self.ui.lineEdit_6.returnPressed.connect(self.ui.pushButton_3.click)
        self.ui.lineEdit_7.returnPressed.connect(self.ui.pushButton_3.click)
        self.ui.pushButton_4.clicked.connect(self.change_status)
        self.ui.pushButton_5.clicked.connect(self.change_level)
        self.ui.pushButton_6.clicked.connect(self.delete_account)
        self.ui.pushButton_7.clicked.connect(self.change_password)

        # 主畫面 - 按鈕連結設定
        self.ui.pushButton_34.clicked.connect(self.set_alarm_ch1)
        self.ui.pushButton_43.clicked.connect(self.set_alarm_ch2)
        self.ui.pushButton_44.clicked.connect(self.set_alarm_ch3)
        self.ui.pushButton_45.clicked.connect(self.set_alarm_ch4)
        self.ui.pushButton_35.clicked.connect(self.alarm_reset)

        # 報表 - 按鈕連結設定
        self.ui.pushButton_22.clicked.connect(self.search_data)
        self.ui.pushButton_23.clicked.connect(self.export_csv)

        # 圖表 - 按鈕連結設定
        self.ui.pushButton_25.clicked.connect(self.search_data_C)
        self.ui.pushButton_26.clicked.connect(self.export_img)

        # 連線 - 按鈕連結設定
        self.ui.pushButton_27.clicked.connect(self.list_connect)
        self.ui.pushButton_28.clicked.connect(self.list_connect_stop)
        self.ui.pushButton_29.clicked.connect(self.add_ip_port)
        self.ui.pushButton_30.clicked.connect(self.del_ip_port)
        self.ui.pushButton_31.clicked.connect(self.load_connect)
        self.ui.pushButton_37.clicked.connect(self.single_connect)
        self.ui.pushButton_20.clicked.connect(self.set_phoenix)
        self.ui.pushButton_18.clicked.connect(self.select_all)
        self.ui.pushButton_19.clicked.connect(self.set_get_interval)
        self.ui.pushButton_21.clicked.connect(self.cancel_all)

        # 軟體記錄 - 按鈕連結設定
        self.ui.pushButton_32.clicked.connect(self.load_eventlog)
        self.ui.pushButton_33.clicked.connect(self.export_excel)

        # 設備記錄 - 按鈕連結設定
        self.ui.pushButton_24.clicked.connect(self.load_eqeventlog)
        self.ui.pushButton_36.clicked.connect(self.export_excel_eq)

        # 異常記錄 - 按鈕連結設定
        self.ui.pushButton_73.clicked.connect(self.load_alarmrecordlog)

        # 異常警報 - 按鈕連結設定
        self.ui.pushButton_8.clicked.connect(self.alarm_record_clr)
        self.ui.pushButton_9.clicked.connect(self.check_alarm_status)

        # 設備執行

    def update_RTC(self):  # 更新軟體時間
        currentTime = QtCore.QDateTime.currentDateTime()
        setcurrentTime = currentTime.toString('yyyy.MM.dd HH:mm:ss')
        self.ui.lcdNumber_2.display(setcurrentTime)

    def setting_para_compelete(self, text):  # 設定完成彈跳對話視窗
        msg = QMessageBox()
        msg.setStandardButtons(QMessageBox.Ok)
        msg.setWindowTitle("訊息")
        msg.setIcon(QMessageBox.Information)
        msg.setInformativeText(text)
        msg.setStyleSheet("font-size:12pt")
        msg.exec_()

    def alarm_msg(self, alarm_text):  # 警告彈跳對話視窗
        msg = QMessageBox()
        msg.setStandardButtons(QMessageBox.Abort)
        msg.setWindowTitle("警告")
        msg.setIcon(QMessageBox.Warning)
        msg.setInformativeText(alarm_text)
        msg.setStyleSheet("font-size:12pt")
        msg.exec_()

    def button_setup(self, button_list):  # 主畫面按鈕設定
        sql = f'SELECT * FROM public.device_button_setup'
        check_list = self.session.execute(sql).fetchall()
        n = 0
        m = 0
        for i in range(len(button_list)):
            for j in check_list:
                if button_list[i] == j[3]:
                    del button_list[i]
                    button_list.insert(0, "")
                    n += 1
        for k in check_list:
            if k[3] != k[0]:
                m += 1
        if n != m:
            for h in range(abs(n - m)):
                button_list.insert(0, "")
        button_list = button_list[:8]
        if button_list[0] != "" and check_list[0][3] == check_list[0][0]:
            self.subMainForm1 = subMainForm(button_list[0], self.session)
            self.subMainForm1.setWindowTitle(f"{button_list[0]} 監控頁面")
            self.ui.pushButton_10.clicked.connect(self.subMainForm1.show)
            sql = f"""UPDATE public.device_button_setup SET serial='{button_list[0]}' WHERE subMainForm='subMainForm1'"""
            self.session.execute(sql)
        else:
            pass

    # page 帳戶資訊 ：帳戶資訊頁面，包含登入登出功能、修改密碼與權限功能、刪除帳號功能，不同權限登入使用不同功能。
    def login(self):  # 登入功能
        self.ui.textBrowser_5.clear()
        login_id = self.ui.lineEdit.text()
        password = self.ui.lineEdit_2.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{login_id}'"""
        list1 = self.session.execute(sql).fetchall()
        sql = f"""SELECT count FROM public.device_check_status WHERE item='login'"""
        num = eval(self.session.execute(sql).fetchall()[0][0])
        if len(list1) == 0:
            self.ui.textBrowser_5.setText(f'無此帳號')
            self.log_login(0, "")
        elif len(list1) > 0:
            if login_id == "":
                self.ui.textBrowser_5.setText(f'帳號不可為空白!!')
                return
            if password == "":
                self.ui.textBrowser_5.setText(f'密碼不可為空白!!')
            elif list1[0][3] == "lock":
                self.ui.textBrowser_5.setText(f'帳號已鎖！請聯繫管理員！')
                return
            elif password != list1[0][1] and num > 0:
                self.ui.textBrowser_5.setText(f'密碼錯誤！請確認密碼！')
                num -= 1
                if num > 0:
                    self.ui.textBrowser_5.setText(f'密碼錯誤！你還有 {str(num)} 次機會!!')
                    sql = f"""UPDATE public.device_check_status SET count='{str(num)}' WHERE item='login'"""
                    self.session.execute(sql)
                    self.log_login(1, list1[0][2])
                elif num == 0 and list1[0][3] == "unlock" and list1[0][2] != "管理員":
                    sql = f"""UPDATE public.device_check_status SET count='3' WHERE item='login'"""
                    self.session.execute(sql)
                    sql = f"""UPDATE public.device_login SET status='lock' WHERE id='{login_id}'"""
                    self.session.execute(sql)
                    self.ui.textBrowser_5.setText(f'帳號已鎖！請聯繫管理員！')
                    self.log_login(2, list1[0][2])
                elif num == 0 and list1[0][3] == "unlock" and list1[0][2] == "管理員":
                    self.ui.textBrowser_5.setText(f'程式即將關閉！')
                    sql = f"""UPDATE public.device_check_status SET count='3' WHERE item='login'"""
                    self.session.execute(sql)
                    self.log_login(4, list1[0][2])
                    time.sleep(3)
                    self.close()
            elif password == list1[0][1] and list1[0][2] == "管理員" and list1[0][3] == "unlock" and num > 0:
                self.ui.tab_2.setEnabled(True)
                self.ui.tab_3.setEnabled(True)
                self.ui.tab_4.setEnabled(True)
                self.ui.tab_5.setEnabled(True)
                self.ui.tab_6.setEnabled(True)
                self.ui.tab_7.setEnabled(True)
                self.ui.tab_8.setEnabled(True)
                self.ui.tab_9.setEnabled(True)
                self.ui.groupBox.setEnabled(True)
                self.ui.groupBox_2.setEnabled(True)
                self.ui.groupBox_8.setEnabled(True)
                self.ui.textBrowser.setText(str(list1[0][0]))
                self.ui.textBrowser_2.setText(str(list1[0][2]))
                self.ui.lineEdit.setEnabled(False)
                self.ui.lineEdit_2.setEnabled(False)
                self.ui.pushButton.setEnabled(False)
                self.ui.pushButton_8.setEnabled(True)
                self.ui.textBrowser_5.setText(f'想再次登入帳號前請先登出!!')
                self.log_login(3, list1[0][2])
                sql = f"""UPDATE public.device_check_status SET count='0' WHERE item='colorlight_red'"""
                self.session.execute(sql)
                sql = f"""UPDATE public.device_check_status SET count='0' WHERE item='colorlight_yellow'"""
                self.session.execute(sql)
                sql = f'SELECT * FROM public.device_login'
                list1 = self.session.execute(sql).fetchall()
                index = 0
                for i in list1:
                    self.ui.listWidget.insertItem(index, f"""{i[0]}\t{i[2]}\t{i[3]}""")
                self.ui.listWidget.sortItems()
            elif password == list1[0][1] and list1[0][2] == "工程師" and list1[0][3] == "unlock" and num > 0:
                self.ui.tab_2.setEnabled(True)
                self.ui.tab_3.setEnabled(True)
                self.ui.tab_4.setEnabled(True)
                self.ui.tab_5.setEnabled(True)
                self.ui.tab_6.setEnabled(True)
                self.ui.tab_7.setEnabled(True)
                self.ui.tab_8.setEnabled(True)
                self.ui.tab_9.setEnabled(True)
                self.ui.groupBox_2.setEnabled(True)
                self.ui.textBrowser.setText(str(list1[0][0]))
                self.ui.textBrowser_2.setText(str(list1[0][2]))
                self.ui.lineEdit.setEnabled(False)
                self.ui.lineEdit_2.setEnabled(False)
                self.ui.pushButton_8.setEnabled(True)
                self.ui.textBrowser_5.setText(f'想再次登入帳號前請先登出!!')
                self.log_login(3, list1[0][2])
            elif password == list1[0][1] and list1[0][2] == "操作員" and list1[0][3] == "unlock" and num > 0:
                self.ui.tab_2.setEnabled(True)
                self.ui.tab_3.setEnabled(True)
                self.ui.tab_4.setEnabled(True)
                self.ui.tab_5.setEnabled(True)
                self.ui.tab_6.setEnabled(True)
                self.ui.tab_7.setEnabled(True)
                self.ui.tab_8.setEnabled(True)
                self.ui.tab_9.setEnabled(True)
                self.ui.groupBox_2.setEnabled(True)
                self.ui.groupBox_3.setEnabled(False)
                self.ui.groupBox_8.setEnabled(False)
                self.ui.pushButton_29.setEnabled(False)
                self.ui.pushButton_30.setEnabled(False)
                self.ui.textBrowser.setText(str(list1[0][0]))
                self.ui.textBrowser_2.setText(str(list1[0][2]))
                self.ui.lineEdit.setEnabled(False)
                self.ui.lineEdit_2.setEnabled(False)
                self.ui.pushButton_8.setEnabled(True)
                self.ui.textBrowser_5.setText(f'想再次登入帳號前請先登出!!')
                self.log_login(3, list1[0][2])

    # 登出功能：登出後關閉所有程式功能
    def logout(self):
        self.log_logout()
        self.ui.tab_2.setEnabled(False)
        self.ui.tab_3.setEnabled(False)
        self.ui.tab_4.setEnabled(False)
        self.ui.tab_5.setEnabled(False)
        self.ui.tab_6.setEnabled(False)
        self.ui.tab_7.setEnabled(False)
        self.ui.tab_8.setEnabled(False)
        self.ui.tab_9.setEnabled(False)
        self.ui.tabWidget.removeTab(9)
        self.ui.groupBox.setEnabled(False)
        self.ui.groupBox_2.setEnabled(False)
        self.ui.groupBox_8.setEnabled(False)
        self.ui.textBrowser.clear()
        self.ui.textBrowser_2.clear()
        self.ui.textBrowser_5.clear()
        self.ui.lineEdit.clear()
        self.ui.lineEdit_2.clear()
        self.ui.lineEdit_5.clear()
        self.ui.lineEdit_6.clear()
        self.ui.lineEdit_7.clear()
        self.ui.lineEdit_9.clear()
        self.ui.lineEdit_10.clear()
        self.ui.lineEdit_11.clear()
        self.ui.listWidget.clear()
        self.ui.lineEdit.setEnabled(True)
        self.ui.lineEdit_2.setEnabled(True)
        self.ui.pushButton.setEnabled(True)
        sql = f"""UPDATE public.device_check_status SET count='3' WHERE item='login'"""
        self.session.execute(sql)

    # 新增帳戶功能：僅「管理者」開放此功能
    def add_account(self):
        try:
            id01 = self.ui.lineEdit_5.text()
            password = self.ui.lineEdit_6.text()
            confirm = self.ui.lineEdit_7.text()
            level = self.ui.comboBox.currentText()
            if id01 == "":
                self.ui.textBrowser_5.setText(f'帳號不可為空白!!')
                return
            elif password == "":
                self.ui.textBrowser_5.setText(f'密碼不可為空白!!')
                return
            if password == confirm:
                msg = QMessageBox()
                msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
                msg.setWindowTitle("再次確認")
                msg.setIcon(QMessageBox.Warning)
                msg.setInformativeText("確認是否要加入此帳號？")
                ans = msg.exec_()
                if ans == QMessageBox.Yes:
                    pass
                elif ans == QMessageBox.No:
                    return
                sql = f"""INSERT INTO public.device_login ("id","password","level","status") VALUES ('{id01}','{password}','{level}','unlock')  ON CONFLICT ("id") DO NOTHING"""
                self.session.execute(sql)
                self.ui.textBrowser_5.setText(f"""成功加入帳號!!""")
                self.ui.listWidget.clear()
                sql = f"""SELECT * FROM public.device_login"""
                list1 = self.session.execute(sql).fetchall()
                index = 0
                for i in list1:
                    self.ui.listWidget.insertItem(index, f"""{i[0]}\t{i[2]}\t{i[3]}""")
                self.ui.listWidget.sortItems()
                self.log_add_acount()
            elif password != confirm:
                self.ui.textBrowser_5.setText(f"""密碼確認錯誤""")
            elif password != "" and confirm == "":
                self.ui.textBrowser_5.setText(f"""密碼確認錯誤""")

        except Exception as e:
            self.ui.textBrowser_5.setText(f"""帳號已存在，請改用其他帳號名稱!!""")

    # 修改密碼功能
    def change_password(self):
        id01 = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id like '{id01}'"""
        list1 = self.session.execute(sql).fetchall()
        oldpassword = self.ui.lineEdit_9.text()
        newpassword = self.ui.lineEdit_10.text()
        confirm = self.ui.lineEdit_11.text()
        # level=self.ui.comboBox.currentText()
        if newpassword == confirm and oldpassword == list1[0][1]:
            sql = f"""UPDATE public.device_login SET password='{newpassword}' WHERE id='{id01}'"""
            self.session.execute(sql)
            self.ui.textBrowser_5.setText(f'密碼變更成功！')
            self.log_change_password()
        elif newpassword != confirm:
            self.ui.textBrowser_5.setText(f'請確認密碼！')

    # 解除鎖定功能：密碼打錯3次會鎖定帳號，只有「管理員」可解除鎖定
    def change_status(self):
        if self.ui.listWidget.currentItem() is None:
            self.ui.textBrowser_5.setText(f'若想解鎖，請先選擇帳號')
            return
        else:
            msg = QMessageBox()
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg.setWindowTitle("再次確認")
            msg.setIcon(QMessageBox.Warning)
            msg.setInformativeText("是否確定解鎖此帳號？")
            ans = msg.exec_()
            if ans == QMessageBox.Yes:
                pass
            elif ans == QMessageBox.No:
                return
            id01 = self.ui.listWidget.currentItem().text()
            id01 = list(id01)
            n = 0
            for i in id01:
                if i == "\t":
                    break
                n += 1
            id01 = "".join(id01[:n])
            sql = f'UPDATE public.device_login SET status="unlock" WHERE id="{id01}"'
            self.session.execute(sql)

            self.ui.textBrowser_5.setText(f'成功解鎖!!')
            self.log_unlocked()
            self.ui.listWidget.clear()
            sql = f'SELECT * FROM public.device_login'
            list1 = self.session.execute(sql).fetchall()
            index = 0
            for i in list1:
                self.ui.listWidget.insertItem(index, f"""{i[0]}\t{i[2]}\t{i[3]}""")
            self.ui.listWidget.sortItems()

    # 修改權限功能：修改「管理者」、「工程師」、「操作員」等權限（管理員限定）
    def change_level(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if self.ui.listWidget.currentItem() is None:
            self.ui.textBrowser_5.setText(f'若想變更權限，請先選擇帳號！')
            return
        else:
            msg = QMessageBox()
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg.setWindowTitle("Check Again")
            msg.setIcon(QMessageBox.Warning)
            msg.setInformativeText("是否確定變更此帳號權限？")
            ans = msg.exec_()
            if ans == QMessageBox.Yes:
                pass
            elif ans == QMessageBox.No:
                return
            id01 = self.ui.listWidget.currentItem().text()
            id01 = list(id01)
            n = 0
            for i in id01:
                if i == "\t":
                    break
                n += 1
            id01 = "".join(id01[:n])
            level01 = self.ui.comboBox_2.currentText()
            # 寫入LOG
            log_id = self.ui.lineEdit.text()
            sql = f"""SELECT * FROM public.device_login WHERE id='{log_id}'"""
            list1 = self.session.execute(sql).fetchall()
            level = list1[0][2]
            unlocked_id = id01
            sql = f"""SELECT * FROM public.device_login WHERE id='{unlocked_id}'"""
            list2 = self.session.execute(sql).fetchall()
            old_level = list2[0][2]
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}變更權限 - 帳號：{unlocked_id}；權限：{old_level}-->{level01}') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
            sql = f"""UPDATE public.device_login SET level='{level01}' WHERE id='{id01}'"""
            self.session.execute(sql)
            self.ui.textBrowser_5.setText(f'權限變更成功!!')
            self.ui.listWidget.clear()
            sql = f'SELECT * FROM public.device_login'
            list1 = self.session.execute(sql).fetchall()
            index = 0
            for i in list1:
                self.ui.listWidget.insertItem(index, f"""{i[0]}\t{i[2]}\t{i[3]}""")
            self.ui.listWidget.sortItems()

    # 移除帳號：僅管理員可以操作此功能
    def delete_account(self):
        if self.ui.listWidget.currentItem() is None:
            self.ui.textBrowser_5.setText(f'若想刪除帳號，請先選擇帳號！')
            return
        else:
            msg = QMessageBox()
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg.setWindowTitle("再次確認")
            msg.setIcon(QMessageBox.Warning)
            msg.setInformativeText("是否確定刪除此帳號？")
            ans = msg.exec_()
            if ans == QMessageBox.Yes:
                pass
            elif ans == QMessageBox.No:
                return
            id01 = self.ui.listWidget.currentItem().text()
            id01 = list(id01)
            n = 0
            for i in id01:
                if i == "\t":
                    break
                n += 1
            id01 = "".join(id01[:n])
            sql = f"""DELETE FROM public.device_login WHERE id='{id01}'"""
            self.session.execute(sql)
            self.ui.textBrowser_5.setText(f'帳號刪除成功!!')
            self.log_del_account()
            self.ui.listWidget.clear()
            sql = f'SELECT * FROM public.device_login'
            list1 = self.session.execute(sql).fetchall()
            index = 0
            for i in list1:
                self.ui.listWidget.insertItem(index, f"""{i[0]}\t{i[2]}\t{i[3]}""")
            self.ui.listWidget.sortItems()

    # 從資料庫提取最新一筆資料
    def _get_last_data(self, serial):
        data = pd.read_sql(f"""SELECT * FROM public."{serial}" order by timedate""", self.session.bind)
        data = data.tail(1)
        data_res = data.to_numpy()
        return data_res

    # 更新主畫面資訊
    def update_data(self):
        sql = f"""SELECT * FROM public.device_button_setup ORDER BY 1"""
        check_list = self.session.execute(sql).fetchall()
        sql = f"""SELECT count FROM public.device_check_status WHERE item ~~ 'alarm_%%'"""
        alarm_count = self.session.execute(sql).fetchall()
        alarm_ch1 = int(alarm_count[0][0])
        alarm_ch2 = int(alarm_count[1][0])
        alarm_ch3 = int(alarm_count[2][0])
        alarm_ch4 = int(alarm_count[3][0])
        if check_list[0][3] != check_list[0][0]:
            data_res = self._get_last_data(check_list[0][3])
            self.ui.pushButton_10.setText(
                f"""序號：{str(check_list[0][3])}\n0.3um：{str(data_res[0][1])}\n0.5um：{str(data_res[0][2])}\n1um：{str(data_res[0][3])}\n5um：{str(data_res[0][4])}\n流量：{str(data_res[0][10])}\t狀態：{str(data_res[0][11])}""")
            if data_res[0][10] == "OK" and data_res[0][11] == 'OK' and data_res[0][1] < alarm_ch1 and data_res[0][
                2] < alarm_ch2 and data_res[0][3] < alarm_ch3 and data_res[0][4] < alarm_ch4:
                self.ui.pushButton_10.setStyleSheet("background-color:#00FF7F;font-size:14pt;font-family:微軟正黑體")
            elif data_res[0][10] == "Alert" or data_res[0][11] == 'Alert' or data_res[0][1] > alarm_ch1 or data_res[0][
                2] > alarm_ch2 or data_res[0][3] > alarm_ch3 or data_res[0][4] > alarm_ch4:
                self.ui.pushButton_10.setStyleSheet("background-color:#FF1493;font-size:14pt;font-family:微軟正黑體")
        else:
            pass

    # 設定ALERM參數
    def set_alarm(self):
        self.log_Alarm_reset(2)
        text = '0.3um警報設定完成!'
        alarm_count = self.ui.lineEdit_3.text()
        sql = f'UPDATE public.device_check_status SET count="{alarm_count}" WHERE item="alarm_ch1"'
        self.session.execute(sql)
        self.setting_para_compelete(text)

    # 重置警報
    def alarm_reset(self):
        self.log_Alarm_reset(1)
        sql = f'SELECT * FROM public.device_connect'
        list1 = self.session.execute(sql).fetchall()
        for i in range(len(list1) - 1, -1, -1):
            if self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                pass
            elif not self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                del list1[i]
        self.cancel_all()
        self.list_connect_stop()
        try:
            self.alarm_client.write_register(74, 1)
            self.alarm_client.close()
        except:
            pass
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.ui.tabWidget.setCurrentIndex(8)
        self.ui.listWidget_2.insertItem(0, f"""{now_time}\t暫存下載中，請稍待...""")
        self.ui.listWidget_2.item(0).setBackground(QtGui.QColor('#00A600'))
        self.threadpool.clear()
        sql = f'SELECT * FROM buttonsetup'
        check_list = self.session.execute(sql).fetchall()
        if check_list[0][3] is not None:
            self.ui.pushButton_10.setText('')
            self.ui.pushButton_10.setStyleSheet("background-color:rgb(200, 200, 200)")
        else:
            pass
        for ip, model, serial in list1:
            sql = f"""SELECT ROWID FROM public.device_connect WHERE serial='{serial}'"""
            idx = self.session.execute(sql).fetchall()[0][0] - 1
            self.ui.tableWidget_2.cellWidget(idx, 3).findChild(type(QtWidgets.QCheckBox())).setChecked(True)
        if list1:
            self.threadpool.clear()
            self.ReloadBuffer_TEST = ReloadBuffer(list1, self.session)
            self.threadpool.start(self.ReloadBuffer_TEST)

    # 「報表」頁面
    def search_data(self):
        self.ui.tableWidget.clear()
        self.ui.tableWidget.setColumnCount(0)
        self.ui.tableWidget.setRowCount(0)
        head1 = ['time', 'Sample time', 'Hold time', 'HV', 'TV', 'Flow', 'Service']
        start = self.ui.dateTimeEdit.text()
        end = self.ui.dateTimeEdit_2.text()
        search_table = self.ui.comboBox_3.currentText()
        if search_table == "":
            text = '無報表選項，請先至"連線"分頁點選"讀取"按鈕，再重新操作.'
            self.alarm_msg(text)
            return
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        startstamp = int(time.mktime(starttimeArray))
        endstamp = int(time.mktime(endtimeArray))
        if startstamp > endstamp:
            text = '起始時間不可大於終止時間，請重新設定.'
            self.alarm_msg(text)
            return
        check_size = list()
        check_size = "\",\"".join(check_size)
        sql = f"""SELECT timedate as time,"{check_size}","Sample time","Hold time","HV","TV","Flow","Service" FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by 1 DESC"""
        list1 = pd.read_sql(sql, self.session.bind)
        list1 = list1.to_numpy()
        self.ui.tableWidget.setColumnCount(len(head1))
        self.ui.tableWidget.setHorizontalHeaderLabels(head1)
        self.table_display(list1)
        self.log_table()

    # list轉tablewidget顯示
    def table_display(self, items):
        for i in range(len(items)):
            item = items[i]
            row = self.ui.tableWidget.rowCount()
            self.ui.tableWidget.insertRow(row)
            for j in range(len(item)):
                item = QtWidgets.QTableWidgetItem(str(items[i][j]))
                item.setTextAlignment(Qt.AlignHCenter)
                self.ui.tableWidget.setItem(row, j, item)
        self.ui.tableWidget.resizeColumnsToContents()
        self.ui.tableWidget.resizeRowsToContents()
        self.ui.tableWidget.setStyleSheet("background-color: rgb(255, 255, 255);\n"
                                          "font: 11pt \"微軟正黑體\";")

    # 報表輸出CSV檔
    def export_csv(self):
        sql = f"""select tablename from pg_tables where tablename ~~ '%%S%%'"""
        table_list = self.session.execute(sql).fetchall()
        save_list = []
        for i in range(len(table_list)):
            if self.qCheckBox[i].isChecked():
                save_list.append(self.qCheckBox[i].text())
        if not save_list:
            text = '無選取匯出設備，請選擇匯出設備'
            self.alarm_msg(text)
            return
        filename = QFileDialog.getExistingDirectory()
        reply = QMessageBox.question(self,
                                     '排程',
                                     "是否執行排程匯出？",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            interval_aps, OK = QInputDialog.getText(self, '設定匯出頻率', '請輸入排程時間（分）：')
            file_interval_aps, OK = QInputDialog.getText(self, '設定資料間隔', '請輸入數據間隔時間（小時）：')
            trigger_Job = IntervalTrigger(minutes=int(interval_aps))
            self.schedule.add_job(self.export_csv_on_schedule, trigger=trigger_Job, id='export_csv',
                                  args=[filename, file_interval_aps, save_list],
                                  replace_existing=True, next_run_time=datetime.now(), jitter=5)
            text = 'CSV定時匯出設定完成'
            self.setting_para_compelete(text)
        elif reply == QMessageBox.No:
            start = self.ui.dateTimeEdit.text()
            end = self.ui.dateTimeEdit_2.text()
            starttimeArray = datetime.strptime(start, "%Y-%m-%d %H:%M")
            endtimeArray = datetime.strptime(end, "%Y-%m-%d %H:%M")
            if starttimeArray > endtimeArray:
                text = '起始時間不可大於終止時間，請重新設定。'
                self.alarm_msg(text)
                return
            trigger_onetime = DateTrigger(datetime.now())
            self.schedule.add_job(self.export_csv_one_time, trigger=trigger_onetime, id='export_csv',
                                  args=[filename, save_list, start, end],
                                  replace_existing=True, next_run_time=datetime.now(), jitter=5)
            text = 'CSV匯出'
            self.setting_para_compelete(text)

    # 輸出CSV內容處理
    def export_csv_one_time(self, filename, save_list, start, end):
        for search_table in save_list:
            head1 = ['time', 'Sample time', 'Hold time', 'HV', 'TV', 'Flow', 'Service']
            check_size = list()
            check_size = "\",\"".join(check_size)
            sql = f"""SELECT timedate as time,"{check_size}","Sample time","Hold time","HV","TV","Flow","Service" 
            FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by 1 DESC """
            list1 = pd.read_sql(sql, self.session.bind)
            try:
                save_time = datetime.now().strftime("%Y%m%d%H%M")
                list1.to_csv(f"""{filename}/{search_table}_{save_time}.csv""", index=False)
                self.log_table_2(search_table)
                time.sleep(1)
            except:
                pass

    # 輸出CSV列入排程
    def export_csv_on_schedule(self, filename, delta_time, save_list):
        for search_table in save_list:
            head1 = ['time', 'Sample time', 'Hold time', 'HV', 'TV', 'Flow', 'Service']
            end = datetime.now()
            start = (end - timedelta(hours=eval(delta_time)))
            check_size = list()
            check_size = "\",\"".join(check_size)
            sql = f"""SELECT timedate as time,"{check_size}","Sample time","Hold time","HV","TV","Flow","Service" 
            FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by 1 DESC """
            list1 = pd.read_sql(sql, self.session.bind)
            try:
                save_time = datetime.now().strftime("%Y%m%d%H%M")
                list1.to_csv(f"""{filename}/{search_table}_{save_time}.csv""", index=False)
                self.log_table_2(search_table)
                time.sleep(1)
            except:
                pass

    ## 「圖表」頁面
    # 顯示查詢圖表（Chart)
    def search_data_C(self):
        self.ui.graphicsView.plotItem.clear()
        start = self.ui.dateTimeEdit_3.text()
        end = self.ui.dateTimeEdit_4.text()
        search_table = self.ui.comboBox_4.currentText()
        if search_table == "":
            text = '無圖表選項，請先至"連線"分頁點選"讀取"按鈕，再重新操作.'
            self.alarm_msg(text)
            return
        starttimeArray = datetime.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = datetime.strptime(end, "%Y-%m-%d %H:%M")
        if starttimeArray > endtimeArray:
            text = '起始時間不可大於終止時間，請重新設定.'
            self.alarm_msg(text)
            return
        sql = f"""SELECT count FROM public.device_check_status WHERE item ~~ 'alarm_%%'"""
        alarm = self.session.execute(sql).fetchall()
        alarm_ch = int(alarm[0][0])
        sql = f"""SELECT * FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by timedate"""
        list1 = pd.read_sql(sql, self.session.bind)
        time_key = list1['timedate'].values.tolist()
        list1['timedate'] = list1['timedate'].apply(lambda x: x.strftime('%H:%M'))
        time_str = list1['timedate'].to_numpy()
        time_dic = {}
        for i in range(len(time_str)):
            time_dic[time_key[i] / (10 ** 18)] = time_str[i].replace(" ", "\n")
        self.ui.stringaxis.setTicks([time_dic.items()])
        if self.ui.checkBox_9.isChecked():
            self.ui.graphicsView.plot(list(time_dic.keys()), list1['0.3um'].to_numpy(), pen=mkPen('#9F4D95', width=2),
                                      name='0.3um')
            self.hline_1 = InfiniteLine(angle=0, movable=False, pen=mkPen('r', width=3), label='0.3 Alarm')
            self.hline_1.setY(alarm_ch)
            self.hline_1.label.setPosition(0.2)
            self.ui.graphicsView.plotItem.addItem(self.hline_1)
        else:
            pass
        self.ui.graphicsView.plotItem.enableAutoRange(ViewBox.XAxis)
        self.ui.graphicsView.plotItem.enableAutoRange(ViewBox.YAxis)
        self.log_chart()

    # 輸出圖表
    def export_img(self):
        sql = f"""select tablename from pg_tables where schemaname='fms01' and tablename ~~ '%%S%%'"""
        table_list = self.session.execute(sql).fetchall()
        save_list = []
        for i in range(len(table_list)):
            if self.qCheckBox_2[i].isChecked():
                save_list.append(self.qCheckBox_2[i].text())
        if not save_list:
            text = '無選取匯出設備，請選擇匯出設備'
            self.alarm_msg(text)
            return
        filename = QFileDialog.getExistingDirectory()
        if filename == '':
            return
        else:
            pass
        reply = QMessageBox.question(self,
                                     '排程',
                                     "是否執行排程匯出？",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            interval_aps, OK = QInputDialog.getText(self, '設定匯出頻率', '請輸入排程時間（分）：')
            file_interval_aps, OK = QInputDialog.getText(self, '設定資料間隔', '請輸入數據間隔時間（小時）：')
            trigger_Job = IntervalTrigger(minutes=int(interval_aps))
            self.schedule.add_job(self.export_img_on_schedule, trigger=trigger_Job, id='export_img',
                                  args=[filename, file_interval_aps, save_list],
                                  replace_existing=True, next_run_time=datetime.now(), jitter=5)
            text = '圖表定時匯出設定完成'
            self.setting_para_compelete(text)
        elif reply == QMessageBox.No:
            start = self.ui.dateTimeEdit_3.text()
            end = self.ui.dateTimeEdit_4.text()
            starttimeArray = datetime.strptime(start, "%Y-%m-%d %H:%M")
            endtimeArray = datetime.strptime(end, "%Y-%m-%d %H:%M")
            if starttimeArray > endtimeArray:
                text = '起始時間不可大於終止時間，請重新設定.'
                self.alarm_msg(text)
                return
            trigger_onetime = DateTrigger(datetime.now())
            self.schedule.add_job(self.export_img_one_time, trigger=trigger_onetime, id='export_img_one_time',
                                  args=[filename, save_list, start, end],
                                  replace_existing=True, next_run_time=datetime.now(), jitter=5)
            text = '圖表匯出'
            self.setting_para_compelete(text)

    # 輸出圖表內容處理
    def export_img_one_time(self, filename, save_list, start, end):
        try:
            for search_table in save_list:
                self.ui.graphicsView.plotItem.clear()
                sql = f"""SELECT count FROM public.device_check_status WHERE item ~~ 'alarm_%%'"""
                alarm = self.session.execute(sql).fetchall()
                alarm_ch = int(alarm[0][0])
                sql = f"""SELECT * FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by timedate"""
                list1 = pd.read_sql(sql, self.session.bind)
                list1['timedate'] = [x.strftime('%H:%M:%S') for x in list1.timedate]
                data_list = []
                color_list = []
                if self.ui.checkBox_9.isChecked():
                    data_list.append('0.3um')
                    color_list.append('b')
                else:
                    pass
                if self.ui.checkBox_10.isChecked():
                    data_list.append('0.5um')
                    color_list.append('orange')
                else:
                    pass
                if self.ui.checkBox_11.isChecked():
                    data_list.append('1um')
                    color_list.append('g')
                else:
                    pass
                if self.ui.checkBox_12.isChecked():
                    data_list.append('5um')
                    color_list.append('r')
                else:
                    pass
                list2 = pd.pivot_table(list1, index='timedate', values=data_list)
                list2.plot(figsize=(16, 8), fontsize=12, style=color_list)
                plt.xlabel('Time', fontsize=18)
                plt.ylabel('Count', fontsize=18)
                plt.legend()
                plt.grid(color='#8E8E8E', linestyle='--', linewidth=1)
                plt.autoscale()
                save_time = datetime.now().strftime("%Y%m%d%H%M")
                save_name = f"""{filename}/{search_table}_{save_time}.png"""
                if os.path.isfile(save_name):
                    os.remove(save_name)
                plt.savefig(save_name)
                plt.clf()
                plt.close('all')
                self.log_chart_2(search_table)
                time.sleep(1)
        except:
            pass

    # 輸出圖表列入排程
    def export_img_on_schedule(self, filename, interval, save_list):
        try:
            for search_table in save_list:
                self.ui.graphicsView.plotItem.clear()
                end = datetime.now()
                start = end - timedelta(hours=int(interval))
                sql = f"""SELECT count FROM public.device_check_status WHERE item ~~ 'alarm_%%'"""
                alarm = self.session.execute(sql).fetchall()
                alarm_ch = int(alarm[0][0])
                sql = f"""SELECT * FROM public."{search_table}" WHERE timedate BETWEEN '{start}' and '{end}' order by timedate"""
                list1 = pd.read_sql(sql, self.session.bind)
                list1['timedate'] = [x.strftime('%H:%M:%S') for x in list1.timedate]
                data_list = []
                color_list = []
                list2 = pd.pivot_table(list1, index='timedate', values=data_list)
                list2.plot(figsize=(16, 8), fontsize=12, style=color_list)
                plt.xlabel('Time', fontsize=18)
                # plt.xticks(rotation=10)
                plt.ylabel('Count', fontsize=18)
                plt.legend()
                plt.grid(color='#8E8E8E', linestyle='--', linewidth=1)
                plt.autoscale()
                save_time = datetime.now().strftime("%Y%m%d%H%M")
                save_name = f"""{filename}/{search_table}_{save_time}.png"""
                if os.path.isfile(save_name):
                    os.remove(save_name)
                plt.savefig(save_name)
                plt.clf()
                plt.close('all')
                self.log_chart_2(search_table)
                time.sleep(1)
        except Exception as e:
            print(e)
            pass

    ## 「連線」頁面
    # 設備單機連線
    def single_connect(self):
        if self.ui.tableWidget_2.currentItem() is None:
            text = '請點選擇想連線的IP位置'
            self.alarm_msg(text)
        else:
            sql = f"""SELECT count From public.device_check_status WHERE item='interval'"""
            get_interval = self.session.execute(sql).fetchall()[0][0]
            sql = f'SELECT * FROM public.device_connect'
            list1 = self.session.execute(sql).fetchall()
            cho_index = self.ui.tableWidget_2.currentRow()
            status_con = {list1[cho_index][0]: cho_index}
            ip = list1[cho_index][0]
            serial = list1[cho_index][2]
            button_list = [serial]
            for k in range(8 - len(button_list)):
                button_list.append("")
            self.button_setup(button_list)
            self.ui.tableWidget_2.cellWidget(cho_index, 3).findChild(type(QtWidgets.QCheckBox())).setChecked(True)
            trigger_Job = IntervalTrigger(seconds=int(get_interval))
            self.schedule.add_job(self.Worker, trigger=trigger_Job, id=f'{serial}', args=[ip, serial, status_con],
                                  replace_existing=True, next_run_time=datetime.now(), jitter=5)

    # 設備多台連線
    def list_connect(self):
        button_list = []
        unlicensed_list = []
        sql = f'SELECT * FROM public.device_connect'
        list1 = self.session.execute(sql).fetchall()
        sql = f"""SELECT count From public.device_check_status WHERE item='interval'"""
        get_interval = self.session.execute(sql).fetchall()[0][0]
        status_con = {}
        for i in range(len(list1) - 1, -1, -1):
            if self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                status_con[list1[i][0]] = i
                pass
            elif not self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                del list1[i]
        if list1:
            for ip, model, serial in list1:
                button_list.append(serial)
            for k in range(8 - len(button_list)):
                button_list.append("")
            self.button_setup(button_list)
            for ip, model, serial in list1:
                trigger_Job = IntervalTrigger(seconds=int(get_interval))
                self.schedule.add_job(self.Worker, trigger=trigger_Job, id=f'{serial}',
                                      args=[ip, serial, status_con], replace_existing=True,
                                      next_run_time=datetime.now(), jitter=5)
            if unlicensed_list:
                text = '\n'.join(unlicensed_list) + '\n未授權，請聯絡供應商。'
                self.alarm_msg(text)
        else:
            text = '請勾選想啟動的設備'
            self.alarm_msg(text)

    # 設備離線
    def list_connect_stop(self):
        sql = f'SELECT * FROM public.device_connect'
        list1 = self.session.execute(sql).fetchall()
        status_con = {}
        for i in range(len(list1) - 1, -1, -1):
            if not self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                status_con[list1[i][2]] = i
                pass
            elif self.ui.tableWidget_2.cellWidget(i, 3).findChild(type(QtWidgets.QCheckBox())).isChecked():
                del list1[i]
        for serial, index in status_con.items():
            try:
                self.schedule.remove_job(f'{serial}')
            except:
                pass
            self.ui.tableWidget_2.item(index, 4).setText(f'離線')
            self.ui.tableWidget_2.item(index, 4).setTextAlignment(QtCore.Qt.AlignCenter)
            self.ui.tableWidget_2.item(index, 4).setBackground(QtGui.QColor(220, 20, 60))
        if self.schedule.get_job('export_csv') is not None and self.schedule.get_job('export_img') is not None:
            self.schedule.remove_job(f"export_csv")
            self.schedule.remove_job(f"export_img")
            text = '匯出報表及圖表排程結束'
            self.setting_para_compelete(text)
        elif self.schedule.get_job('export_csv') is not None:
            self.schedule.remove_job(f"export_csv")
            text = '匯出報表排程結束'
            self.setting_para_compelete(text)
        elif self.schedule.get_job('export_img') is not None:
            self.schedule.remove_job(f"export_img")
            text = '匯出圖表排程結束'
            self.setting_para_compelete(text)

    # 顯示連線結果
    def check_connect_OK(self, it_index):
        self.ui.tableWidget_2.item(it_index, 4).setText(f'連線')
        self.ui.tableWidget_2.item(it_index, 4).setTextAlignment(QtCore.Qt.AlignCenter)
        self.ui.tableWidget_2.item(it_index, 4).setBackground(QtGui.QColor(50, 205, 50))
        self.ui.tableWidget_2.update()

    def check_connect_NG(self, it_index):
        self.ui.tableWidget_2.item(it_index, 4).setText(f'離線')
        self.ui.tableWidget_2.item(it_index, 4).setTextAlignment(QtCore.Qt.AlignCenter)
        self.ui.tableWidget_2.item(it_index, 4).setBackground(QtGui.QColor(220, 20, 60))
        self.ui.tableWidget_2.update()

    # 加入設備IP和PORT
    def add_ip_port(self):
        ip = self.ui.lineEdit_26.text()
        sql = f'SELECT * FROM public.device_connect'
        list1 = self.session.execute(sql).fetchall()
        if ip == "":
            text = "IP不可空白"
            self.alarm_msg(text)
            self.load_connect()
            return
        elif len(list1) > 7:
            text = "IP最多8筆，如需新增請刪除舊的後新增"
            self.alarm_msg(text)
            self.load_connect()
            return
        else:
            ip_l = ip.split('.')
            self.client = ModbusClient(f"""{int(ip_l[0])}.{int(ip_l[1])}.{int(ip_l[2])}.{int(ip_l[3])}""", 502)
            if self.client.connect():
                pass
            elif not self.client.connect():
                text = '請確認設備網路線是否連接'
                self.alarm_msg(text)
                return
            serial = ''
            number = self.client.read_holding_registers(20, 8).registers
            for i in range(0, 8, 2):
                serial += number[i] + number[i + 1]
            self.ui.tableWidget_2.clear()
            sql = f"""INSERT INTO public.device_connect (ip,model,serial) VALUES ('{ip}','{serial}') ON CONFLICT ("ip") DO NOTHING"""
            self.session.execute(sql)
            sql = f"""CREATE TABLE IF NOT EXISTS public."{str(serial)}"
                    (
                        timedate timestamp without time zone NOT NULL,
                        "0.3um" integer NOT NULL,
                        "0.5um" integer NOT NULL,
                        "1um" integer NOT NULL,
                        "5um" integer NOT NULL,
                        "Location" integer NOT NULL,
                        "Sample time" integer,
                        "Hold time" integer,
                        "HV" integer,
                        "TV" integer,
                        "Flow" character varying(50),
                        "Service" character varying(50),
                        PRIMARY KEY (timedate)
                    )"""
            self.session.execute(sql)
            self.client.close()
            self.log_connect(1)
            self.load_connect()

    # 刪除IP和PORT
    def del_ip_port(self):
        if self.ui.tableWidget_2.currentItem() is None:
            text = '若想刪除IP，請先選擇IP'
            self.alarm_msg(text)
            return
        else:
            del_ip = self.ui.tableWidget_2.currentItem().text()
            sql = f'DELETE FROM public.device_connect WHERE ip="{str(del_ip)}"'
            self.session.execute(sql)
            self.log_connect(2)
            self.load_connect()

    # 讀取連線資訊
    def load_connect(self):
        self.ui.comboBox_3.clear()
        self.ui.comboBox_4.clear()
        self.ui.tableWidget_2.clear()
        self.ui.tableWidget_2.setColumnCount(0)
        self.ui.tableWidget_2.setRowCount(0)
        head = ['IP', 'Serial']
        sql = f'SELECT * FROM public.device_connect'
        list1 = self.session.execute(sql).fetchall()
        list1 = pd.DataFrame(list1, columns=head)
        list1 = list1.to_numpy()
        head2 = ['IP', 'Serial', 'Connect', 'Status']
        self.ui.tableWidget_2.setColumnCount(len(head2))
        self.ui.tableWidget_2.setHorizontalHeaderLabels(head2)
        for i in range(len(list1)):
            item = list1[i]
            row = self.ui.tableWidget_2.rowCount()
            self.ui.tableWidget_2.insertRow(row)
            m = len(item)
            for j in range(len(item)):
                item = QtWidgets.QTableWidgetItem(str(list1[i][j]))
                item.setTextAlignment(Qt.AlignHCenter)
                self.ui.tableWidget_2.setItem(row, j, item)
            cell_widget = QtWidgets.QWidget()
            chk_bx = QtWidgets.QCheckBox()
            chk_bx.setCheckState(Qt.Unchecked)
            lay_out = QtWidgets.QHBoxLayout(cell_widget)
            lay_out.addWidget(chk_bx)
            lay_out.setAlignment(Qt.AlignCenter)
            cell_widget.setLayout(lay_out)
            self.ui.tableWidget_2.setCellWidget(row, m, cell_widget)
            status = QtWidgets.QTableWidgetItem('離線')
            self.ui.tableWidget_2.setItem(row, m + 1, status)
            self.ui.tableWidget_2.item(row, m + 1).setBackground(QtGui.QColor(220, 20, 60))
            self.ui.tableWidget_2.item(row, m + 1).setTextAlignment(QtCore.Qt.AlignCenter)
        self.ui.tableWidget_2.resizeColumnsToContents()
        self.ui.tableWidget_2.resizeRowsToContents()
        self.ui.tableWidget_2.setStyleSheet("background-color: rgb(255, 255, 255);\n"
                                            "font: 11pt \"微軟正黑體\";")
        sql = f"""select tablename from pg_tables where schemaname='fms01' and tablename ~~ '%%S%%'"""
        list2 = self.session.execute(sql).fetchall()
        list3 = []
        for n in list2:
            list3.append(n[0])
        self.ui.comboBox_3.addItems(list3)
        self.ui.comboBox_4.addItems(list3)
        self.ui.comboBox_7.addItems(list3)
        self.ui.comboBox_8.addItems(list3)
        self.qCheckBox = []
        self.qCheckBox_2 = []
        qListWidget = QtWidgets.QListWidget()
        qListWidget_2 = QtWidgets.QListWidget()
        qLineEdit = QtWidgets.QLineEdit()
        qLineEdit_2 = QtWidgets.QLineEdit()
        row_num = len(list2)
        for i in range(row_num):
            self.qCheckBox.append(QtWidgets.QCheckBox())
            self.qCheckBox_2.append(QtWidgets.QCheckBox())
            qItem = QtWidgets.QListWidgetItem(qListWidget)
            qItem_2 = QtWidgets.QListWidgetItem(qListWidget_2)
            self.qCheckBox[i].setText(list2[i][0])
            self.qCheckBox_2[i].setText(list2[i][0])
            qListWidget.setItemWidget(qItem, self.qCheckBox[i])
            qListWidget_2.setItemWidget(qItem_2, self.qCheckBox_2[i])
        self.ui.comboBox_5.setLineEdit(qLineEdit)
        self.ui.comboBox_5.setModel(qListWidget.model())
        self.ui.comboBox_5.setView(qListWidget)
        self.ui.comboBox_6.setLineEdit(qLineEdit_2)
        self.ui.comboBox_6.setModel(qListWidget_2.model())
        self.ui.comboBox_6.setView(qListWidget_2)

    def select_all(self):
        sql = f"""SELECT * FROM public.device_connect"""
        list1 = self.session.execute(sql).fetchall()
        for cho_index in range(len(list1)):
            self.ui.tableWidget_2.cellWidget(cho_index, 3).findChild(type(QtWidgets.QCheckBox())).setChecked(True)

    def cancel_all(self):
        sql = f"""SELECT * FROM public.device_connect"""
        list1 = self.session.execute(sql).fetchall()
        for cho_index in range(len(list1)):
            self.ui.tableWidget_2.cellWidget(cho_index, 3).findChild(type(QtWidgets.QCheckBox())).setChecked(False)

    def set_get_interval(self):
        get_interval = self.ui.spinBox.text()
        sql = f"""UPDATE public.device_check_status SET count='{get_interval}' WHERE item='interval'"""
        self.session.execute(sql)
        text = f'取樣間隔{get_interval}秒設定完成！'
        self.setting_para_compelete(text)

    # 設定菲尼克斯
    def set_phoenix(self):
        alarm_count = self.ui.lineEdit_23.text()
        sql = f'UPDATE public.device_check_status SET count="{alarm_count}" WHERE item="phoenix"'
        self.session.execute(sql)
        text = '變更Phoenix設定，請重啟軟體.'
        self.alarm_msg(text)
        return

    # 軟體記錄
    def load_eventlog(self):
        self.ui.tableWidget_3.clear()
        self.ui.tableWidget_3.setColumnCount(0)
        self.ui.tableWidget_3.setRowCount(0)
        start = self.ui.dateTimeEdit_5.text()
        end = self.ui.dateTimeEdit_6.text()
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        if starttimeArray > endtimeArray:
            text = '起始時間不可大於終止時間，請重新設定。'
            self.alarm_msg(text)
            return
        search_keyword = self.ui.comboBox_8.currentText()
        head = ['時間', '帳號', '權限', '訊息']
        if self.ui.checkBox_5.isChecked():
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_soft_even_log WHERE timedate BETWEEN '{start}' and '{end}' ORDER BY 1 DESC""",
                self.session.bind)
        else:
            list1 = pd.read_sql(
                f"""SELECT * FROM eventlog WHERE message ~~ '%%{search_keyword[2:]}%%' BETWEEN '{start}' and '{end}' ORDER BY 1 DESC""",
                self.session.bind)
        list1 = list1.to_numpy()
        self.ui.tableWidget_3.setColumnCount(len(head))
        self.ui.tableWidget_3.setHorizontalHeaderLabels(head)
        for i in range(len(list1)):
            item = list1[i]
            row = self.ui.tableWidget_3.rowCount()
            self.ui.tableWidget_3.insertRow(row)
            for j in range(len(item)):
                item = QtWidgets.QTableWidgetItem(str(list1[i][j]))
                self.ui.tableWidget_3.setItem(row, j, item)
        self.ui.tableWidget_3.resizeColumnsToContents()
        self.ui.tableWidget_3.setStyleSheet("background-color: rgb(255, 255, 255);\n"
                                            "font: 11pt \"微軟正黑體\";")

    # 軟體記錄輸出EXCEL
    def export_excel(self):
        start = self.ui.dateTimeEdit_5.text()
        end = self.ui.dateTimeEdit_6.text()
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        startstamp = int(time.mktime(starttimeArray))
        endstamp = int(time.mktime(endtimeArray))
        if startstamp > endstamp:
            text = '起始時間不可大於終止時間，請重新設定。'
            self.alarm_msg(text)
            return
        search_keyword = self.ui.comboBox_8.currentText()
        head = ['時間', '帳號', '權限', '訊息']
        if self.ui.checkBox_5.isChecked():
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_soft_even_log WHERE timedate BETWEEN '{start}' and '{end}' ORDER BY 1 DESC""",
                self.session.bind)
        else:
            list1 = pd.read_sql(
                f"""SELECT * FROM eventlog WHERE message ~~ '%%{search_keyword[2:]}%%' BETWEEN '{start}' and '{end}' ORDER BY 1 DESC""",
                self.session.bind)
        list1.columns = head
        filename = QFileDialog.getSaveFileName(None, '', '', 'xlsx(*.xlsx)')
        if filename[0] == '':
            return
        else:
            writer = pd.ExcelWriter(filename[0], engine='xlsxwriter')
            list1.to_excel(writer, startrow=0, startcol=0, sheet_name='Sheet1', index=False)
            worksheet = writer.sheets['Sheet1']
            for i, col in enumerate(list1.columns):
                column_len = list1[col].astype(str).str.len().max()
                column_len = max(column_len, len(col)) + 5
                worksheet.set_column(i, i, column_len)
            writer.save()
            text = '軟體記錄匯出完成！'
            self.setting_para_compelete(text)

    # 設備記錄
    def load_eqeventlog(self):
        self.ui.tableWidget_4.clear()
        self.ui.tableWidget_4.setColumnCount(0)
        self.ui.tableWidget_4.setRowCount(0)
        start = self.ui.dateTimeEdit_7.text()
        end = self.ui.dateTimeEdit_8.text()
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        startstamp = int(time.mktime(starttimeArray))
        endstamp = int(time.mktime(endtimeArray))
        if startstamp > endstamp:
            text = '起始時間不可大於終止時間，請重新設定。'
            self.alarm_msg(text)
            return
        search_keyword = self.ui.comboBox_7.currentText()
        head = ['時間', '帳號', '權限', '訊息']
        if self.ui.checkBox_6.isChecked():
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_eq_even_log WHERE timedate BETWEEN '{start}' and '{end}' ORDER BY 1""",
                self.session.bind)
        else:
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_eq_even_log WHERE message ~~ '%{search_keyword[2:]}%%' and timedate BETWEEN '{start}' and '{end}' ORDER BY 1""",
                self.session.bind)
        list1 = list1.to_numpy()
        self.ui.tableWidget_4.setColumnCount(len(head))
        self.ui.tableWidget_4.setHorizontalHeaderLabels(head)
        for i in range(len(list1)):
            item = list1[i]
            row = self.ui.tableWidget_4.rowCount()
            self.ui.tableWidget_4.insertRow(row)
            for j in range(len(item)):
                item = QtWidgets.QTableWidgetItem(str(list1[i][j]))
                self.ui.tableWidget_4.setItem(row, j, item)
        self.ui.tableWidget_4.resizeColumnsToContents()
        self.ui.tableWidget_4.setStyleSheet("background-color: rgb(255, 255, 255);\n"
                                            "font: 11pt \"微軟正黑體\";")

    # 設備記錄輸出EXCEL
    def export_excel_eq(self):
        start = self.ui.dateTimeEdit_7.text()
        end = self.ui.dateTimeEdit_8.text()
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        startstamp = int(time.mktime(starttimeArray))
        endstamp = int(time.mktime(endtimeArray))
        if startstamp > endstamp:
            text = '起始時間不可大於終止時間，請重新設定。'
            self.alarm_msg(text)
            return
        search_keyword = self.ui.comboBox_7.currentText()
        head = ['時間', '帳號', '權限', '訊息']
        if self.ui.checkBox_6.isChecked():
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_eq_even_log WHERE timedate BETWEEN '{start}' and '{end}' ORDER BY 1""",
                self.session.bind)
        else:
            list1 = pd.read_sql(
                f"""SELECT * FROM public.device_eq_even_log WHERE message ~~ '%{search_keyword[2:]}%%' and timedate BETWEEN '{start}' and '{end}' ORDER BY 1""",
                self.session.bind)
        list1.columns = head
        # list1 = list1.to_numpy()
        filename = QFileDialog.getSaveFileName(None, '', '', 'xlsx(*.xlsx)')
        if filename[0] == '':
            return
        else:
            writer = pd.ExcelWriter(filename[0], engine='xlsxwriter')
            list1.to_excel(writer, startrow=0, sheet_name='Sheet1', index=False)
            # workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            for i, col in enumerate(list1.columns):
                column_len = list1[col].astype(str).str.len().max()
                column_len = max(column_len, len(col)) + 5
                worksheet.set_column(i, i, column_len)
            writer.save()
            #             list1.to_excel(filename[0])
            text = '設備記錄匯出完成！'
            self.setting_para_compelete(text)

    # 異常記錄
    def load_alarmrecordlog(self):
        self.ui.tableWidget_10.clear()
        self.ui.tableWidget_10.setColumnCount(0)
        self.ui.tableWidget_10.setRowCount(0)
        start = self.ui.dateTimeEdit_19.text()
        end = self.ui.dateTimeEdit_20.text()
        starttimeArray = time.strptime(start, "%Y-%m-%d %H:%M")
        endtimeArray = time.strptime(end, "%Y-%m-%d %H:%M")
        if starttimeArray > endtimeArray:
            text = '起始時間不可大於終止時間，請重新設定。'
            self.alarm_msg(text)
            return
        head = ['時間', '帳號', '權限', '訊息']
        list1 = pd.read_sql(
            f"""SELECT * FROM public.device_alarm_log WHERE timedate BETWEEN '{start}' and '{end}' ORDER BY 1 DESC""",
            self.session.bind)
        list1 = list1.to_numpy()
        self.ui.tableWidget_10.setColumnCount(len(head))
        self.ui.tableWidget_10.setHorizontalHeaderLabels(head)
        for i in range(len(list1)):
            item = list1[i]
            row = self.ui.tableWidget_10.rowCount()
            self.ui.tableWidget_10.insertRow(row)
            for j in range(len(item)):
                item = QtWidgets.QTableWidgetItem(str(list1[i][j]))
                self.ui.tableWidget_10.setItem(row, j, item)
        self.ui.tableWidget_10.resizeColumnsToContents()
        self.ui.tableWidget_10.setStyleSheet("background-color: rgb(255, 255, 255);\n"
                                             "font: 11pt \"微軟正黑體\";")

    # 異常警報
    def alarm_record_clr(self):
        self.ui.listWidget_2.clear()

    # 確認警報器狀態
    def check_alarm_status(self):
        if self.ui.checkBox_7.isChecked():
            self.ui.label_4.clear()
            try:
                if self.alarm_client.connect():
                    self.ui.label_4.setText(f'  連線  ')
                    self.ui.label_4.setStyleSheet('background-color:#00FF7F')
                    self.ui.label_4.setAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignHCenter)
                    self.alarm_client.write_register(64, 1)
                    self.alarm_client.close()
                elif not self.alarm_client.connect():
                    self.ui.label_4.setText(f'  離線  ')
                    self.ui.label_4.setStyleSheet('background-color:#FF1493')
                    self.ui.label_4.setAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignHCenter)
                    self.alarm_client.close()
            except:
                pass
        else:
            pass

    ## 記錄行為
    # 記錄登入LOG
    def log_login(self, num, level):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = self.ui.lineEdit.text()
        level = level
        if num == 0:
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}無此帳號') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
        elif num == 1:
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}輸入密碼錯誤') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
        elif num == 2:
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}輸入密碼錯誤3次，已封鎖') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
        elif num == 3:
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}登入') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
        elif num == 4:
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}輸入密碼錯誤3次，關閉程式') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)

    # 記錄登出LOG
    def log_logout(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = self.ui.lineEdit.text()
        sql = f"""SELECT level FROM public.device_login WHERE id='{log_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][0]
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}登出') ON CONFLICT ("timedate") DO NOTHING"""
        self.session.execute(sql)

    # 記錄增加帳號LOG
    def log_add_acount(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{log_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        add_id = self.ui.lineEdit_5.text()
        add_level = self.ui.comboBox.currentText()
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}加入帳號：{add_id}；權限：{add_level}') ON CONFLICT ("timedate") DO NOTHING"""
        self.session.execute(sql)

    # 記錄帳號解鎖LOG
    def log_unlocked(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{log_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        unlocked_id = self.ui.listWidget.currentItem().text()
        unlocked_id = list(unlocked_id)
        n = 0
        for i in unlocked_id:
            if i == "\t":
                break
            n += 1
        unlocked_id = "".join(unlocked_id[:n])
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}解鎖帳號：{unlocked_id}') ON CONFLICT ("timedate") DO NOTHING"""
        self.session.execute(sql)

    # 記錄刪除帳號LOG
    def log_del_account(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{log_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        del_id = self.ui.listWidget.currentItem().text()
        del_id = list(del_id)
        n = 0
        for i in del_id:
            if i == "\t":
                break
            n += 1
        del_id = "".join(del_id[:n])
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{log_id}','{level}','{log_id}刪除帳號：{del_id}') ON CONFLICT ("timedate") DO NOTHING"""
        self.session.execute(sql)

    # 記錄變更密碼LOG
    def log_change_password(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        change_pw_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{change_pw_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{change_pw_id}','{level}','{change_pw_id}變更密碼') ON CONFLICT ("timedate") DO NOTHING"""
        self.session.execute(sql)

    # 記錄查詢報表LOG
    def log_table(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        search_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{search_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        start = self.ui.dateTimeEdit.text()
        end = self.ui.dateTimeEdit_2.text()
        search_table = self.ui.comboBox_3.currentText()
        sql = f"""INSERT INTO public.device_soft_even_log (timedate, "id", "level", "message") VALUES ('{now_time}','{search_id}','{level}','{search_id}搜尋{start}到{end}的{search_table}報表')"""
        self.session.execute(sql)

    def log_table_2(self, search_table):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        search_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{search_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        start = self.ui.dateTimeEdit.text()
        end = self.ui.dateTimeEdit_2.text()
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate", "id", "level", "message") VALUES ('{now_time}','{search_id}','{level}','{search_id}匯出{start}到{end}的{search_table}報表')"""
        self.session.execute(sql)

    # 記錄圖表查詢LOG
    def log_chart(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        search_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{search_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        start = self.ui.dateTimeEdit_3.text()
        end = self.ui.dateTimeEdit_4.text()
        search_table = self.ui.comboBox_4.currentText()
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{search_id}','{level}','{search_id}搜尋{start}到{end}的{search_table}趨勢圖')"""
        self.session.execute(sql)

    def log_chart_2(self, search_table):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        search_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{search_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        start = self.ui.dateTimeEdit_3.text()
        end = self.ui.dateTimeEdit_4.text()
        search_table = self.ui.comboBox_4.currentText()
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{search_id}','{level}','{search_id}匯出{start}到{end}的{search_table}趨勢圖')"""
        self.session.execute(sql)

    # 記錄連線LOG
    def log_connect(self, num):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        connect_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{connect_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        if num == 1:
            add_ip = self.ui.lineEdit_26.text()
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{connect_id}','{level}','{connect_id}加入IP：{add_ip}')"""
        elif num == 2:
            del_ip = self.ui.tableWidget_2.currentItem().text()
            sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{connect_id}','{level}','{connect_id}刪除IP：{del_ip}')"""
        self.session.execute(sql)

    def log_id_level(self):
        connect_id = self.ui.lineEdit.text()
        sql = f"""SELECT * FROM public.device_login WHERE id='{connect_id}'"""
        list1 = self.session.execute(sql).fetchall()
        level = list1[0][2]
        return connect_id, level

    # 關閉軟體設定
    def closeEvent(self, event):
        reply = QtWidgets.QMessageBox.question(self,
                                               '離開程式',
                                               "是否要退出程式？",
                                               QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if reply == QtWidgets.QMessageBox.Yes:
            event.accept()
            self.schedule.remove_all_jobs()
            time.sleep(1)
            self.schedule.shutdown(wait=False)
            self.session.close()
            QApplication.closeAllWindows()
            sys.exit(0)
        else:
            event.ignore()

    # 擷取感測器資訊
    def Worker(self, ip, serial, status_con):
        try:
            client = ModbusClient(ip, 502)
            connect_id, level = self.log_id_level()
            now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if client.connect():
                sql = f"""UPDATE public.device_button_setup SET thread='1' WHERE serial='{serial}'"""
                self.session.execute(sql)
                self.check_connect_OK(status_con[ip])
                client.close()
            elif not client.connect():
                try:
                    sql = f"""UPDATE public.device_button_setup SET thread='0' WHERE serial='{serial}'"""
                    self.session.execute(sql)
                    self.check_connect_NG(status_con[ip])
                    self.ui.tabWidget.setCurrentIndex(8)
                    self.ui.listWidget_2.insertItem(0, f"""{now_time}\t{serial}\t設備連線異常，請確認設備狀態""")
                    self.ui.listWidget_2.item(0).setBackground(QtGui.QColor('#FF5151'))
                    sql = f"""INSERT INTO public.device_alarm_log ("timedate","id","level","message") VALUES ('{str(
                        now_time)}','{str(connect_id)}','{str(level)}','{str(
                        serial)}設備連線異常，請確認設備狀態') ON CONFLICT ("timedate") DO NOTHING"""
                    self.session.execute(sql)
                    client.close()
                except Exception as e:
                    print(e)
                return
            temp = 'NA'
            humi = 'NA'
            flow_alert_s = ''
            status_alert_s = ''
            client.write_register(47, 65535)
            client.write_register(48, 65535)
            r_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            result = client.read_holding_registers(26, 8).registers
            ch1, ch2, ch3, ch4 = [result[x + 1] for x in range(0, 8, 2)]
            sample_time = client.read_holding_registers(89, 2).registers
            location = client.read_holding_registers(25).registers[0]
            sql = f"""INSERT INTO public.'{serial}' ("timedate","0.1um","0.5um","3um","5um","Location","Sample time","HV","TV","Flow","Service") VALUES ('{str(r_time)}','{ch1}','{ch2}','{ch3}','{ch4}','{str(location)}','{sample_time}','{str(humi)}','{str(temp)}','{str(flow_alert_s)}','{str(status_alert_s)}') ON CONFLICT ("timedate") DO NOTHING"""
            self.session.execute(sql)
            connect_id = self.ui.lineEdit.text()
            sql = f"""SELECT * FROM public.device_login WHERE id='{connect_id}'"""
            list1 = self.session.execute(sql).fetchall()
            client.close()
        except Exception as e:
            client.close()
            pass


# 讀取感測器ROM資料
class ReloadBuffer(QRunnable, ModbusClient):
    def __init__(self, list_all, main_session):
        super(ReloadBuffer, self).__init__()
        self.list_all = list_all
        self.session = main_session
        self.buffer_update_check = len(self.list_all)
        self.count = len(self.list_all)

    def run(self):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        mainForm.ui.tabWidget.setCurrentIndex(8)
        mainForm.ui.listWidget_2.insertItem(0, f"""{now_time}\t初始化，請稍待...""")
        mainForm.ui.listWidget_2.item(0).setBackground(QtGui.QColor('#00A600'))
        for i in range(1, 9):
            sql = f"""UPDATE public.device_button_setup SET thread='0' WHERE subMainForm='subMainForm{str(i)}'"""
            self.session.execute(sql)
        time.sleep(20)
        if self.list_all:
            while self.buffer_update_check != 0:
                try:
                    count_thread = self.count - self.buffer_update_check
                    now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ip = self.list_all[count_thread][0]
                    serial = self.list_all[count_thread][2]
                    client = ModbusClient(ip, 502, auto_open=True)
                    connect_id = mainForm.ui.lineEdit.text()
                    sql = f'SELECT * FROM public.device_login WHERE id="{connect_id}"'
                    list1 = self.session.execute(sql).fetchall()
                    level = list1[0][2]
                    sql = f'SELECT * FROM public.device_connect'
                    chk = self.session.execute(sql)
                    chk = chk.fetchall()
                    chk = pd.DataFrame(chk)
                    chk_no = chk[chk[0] == ip].index.values
                    mainForm.ui.tabWidget.setCurrentIndex(8)
                    mainForm.ui.listWidget_2.insertItem(0, f"""{now_time}\t{serial}暫存下載中，請稍待...""")
                    mainForm.ui.listWidget_2.item(0).setBackground(QtGui.QColor('#00A600'))
                    sql = f"""INSERT INTO public.device_alarm_log ("timedate","id","level","message") VALUES ('{str(
                        now_time)}','{str(connect_id)}','{str(level)}','暫存下載中，請稍待...') ON CONFLICT ("timedate") DO NOTHING"""
                    self.session.execute(sql)
                    if client.connect():
                        mainForm.check_connect_OK(chk_no[0])
                    elif not client.connect():
                        mainForm.check_connect_NG(chk_no[0])
                    client.write_register(97, 65535)
                    client.write_register(98, 65535)
                    end_count = client.read_holding_registers(53, 2).registers
                    if end_count - 1001 < 0:
                        start_count = 1
                    else:
                        start_count = end_count - 1000
                    if client.connect():
                        for i in range(start_count, end_count + 1):
                            flow_alert_s = ''
                            status_alert_s = ''
                            record_id = client.read_holding_registers(45, 2).registers
                            if record_id == 0:
                                continue
                            else:
                                r_time = client.read_holding_registers(45, 6).registers
                                r_time = f"""{r_time[0]}/{r_time[1]}/{r_time[2]} {r_time[3]}:{r_time[4]}:{r_time[5]}"""
                                if r_time == '2255/255/255 255:255:255':
                                    break
                                result = client.read_holding_registers(26, 8).registers
                                ch1, ch2, ch3, ch4 = [result[x + 1] for x in range(0, 8, 2)]
                                sample_time = client.read_holding_registers(20, 1).registers
                                location = client.read_holding_registers(25).registers[0]
                                sql = f"""INSERT INTO {serial}("timedate","0.3um","0.5um","1um","5um","Location","Sample time","Flow","Service") VALUES ('{str(
                                    r_time)}','0','0','0','0','{str(location)}','{str(sample_time)}','{flow_alert_s}','{status_alert_s}') ON CONFLICT ("timedate") DO NOTHING"""
                                self.session.execute(sql)
                    elif not client.connect():
                        pass
                    mainForm.check_connect_NG(chk_no[0])
                    client.close()
                    self.buffer_update_check -= 1
                    if self.buffer_update_check == 0:
                        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        mainForm.ui.tabWidget.setCurrentIndex(8)
                        mainForm.ui.listWidget_2.insertItem(0, f"""{now_time}\t暫存下載完成，請重新啟動連線。""")
                        mainForm.ui.listWidget_2.item(0).setBackground(QtGui.QColor('#00A600'))
                        time.sleep(15)
                        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sql = f"""INSERT INTO public.device_alarm_log ("timedate","id","level","message") VALUES ('{str(
                            now_time)}','{str(connect_id)}','{str(level)}','暫存下載完成，請重新啟動連線。') ON CONFLICT ("timedate") DO NOTHING"""
                        self.session.execute(sql)
                        client.close()
                        time.sleep(2)
                except Exception as e:
                    time.sleep(10)
                    continue


# 子畫面設定
class subMainForm(QDialog, ModbusClient):
    def __init__(self, serial, main_session, parent=None):
        super(subMainForm, self).__init__(parent)
        self.ui2 = ui_Monitor_software_sub.Ui_Dialog()
        self.ui2.setupUi(self)
        self.serial = serial
        self.session = main_session
        self.initUI()
        self.start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def initUI(self):
        self.ui2.textBrowser.setText(self.serial)
        self.ui2.pushButton.clicked.connect(self.monitor_time)

        self.time = QTimer(self)
        self.time.setInterval(15000)
        self.time.timeout.connect(self.update_data)
        self.time.start()

    def monitor_time(self):
        monitor_time = self.ui2.lineEdit.text()
        sql = f'UPDATE public.device_check_status SET count="{monitor_time}" WHERE item="monitor_time"'
        self.session.execute(sql)
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        monitor_time_id, level = mainForm.log_id_level()
        sql = f"""INSERT INTO public.device_soft_even_log ("timedate","id","level","message") VALUES ('{now_time}','{monitor_time_id}','{level}','{monitor_time_id}變更{self.serial}監控時間：{monitor_time}秒')"""
        self.session.execute(sql)

    def update_data(self):
        self.ui2.graphicsView.plotItem.clear()
        sql = f"""SELECT count FROM public.device_check_status WHERE item='monitor_time'"""
        monitor_time1 = self.session.execute(sql).fetchall()[0][0]
        start_time = datetime.now()
        end_time = (start_time - timedelta(minutes=eval(monitor_time1)))
        sql = f"""SELECT * FROM public."{self.serial}" WHERE timedate BETWEEN '{end_time}' AND '{start_time}' ORDER BY 1 DESC"""
        data = self.session.execute(sql).fetchall()
        if data:
            data = pd.DataFrame(data)
            if len(data) > 0:
                data_res = data.values.tolist()
                self.ui2.textBrowser_2.setText(str(data_res[0][1]))
                self.ui2.textBrowser_3.setText(str(data_res[0][2]))
                self.ui2.textBrowser_4.setText(str(data_res[0][3]))
                self.ui2.textBrowser_5.setText(str(data_res[0][4]))
                self.ui2.textBrowser_6.setText(str(data_res[0][0]))
                mainForm.update_data()
            else:
                pass


if __name__ == '__main__':
    app = QApplication(sys.argv)
    mainForm = MainForm()
    mainForm.show()
    sys.exit(app.exec_())
