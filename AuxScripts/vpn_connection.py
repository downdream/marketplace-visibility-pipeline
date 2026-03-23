# -*- coding: utf-8 -*-

### Python libraries ###
import time
from nordvpn_switcher import initialize_VPN,rotate_VPN,terminate_VPN

### Python scripts ###


'''Connect VPN'''
def connect_VPN(country):
    print(f'Initializing VPN server from {country}')
    country_list = []
    country_list.append(country)
    settings = initialize_VPN(area_input=country_list)
    rotate_VPN(settings)
    time.sleep(5)
    return settings

'''Disconnect VPN'''
def disconnect_VPN(settings):
    terminate_VPN(settings)