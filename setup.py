from setuptools import setup

APP = ['main.py']  # <- если ваш главный файл называется не main.py, замените здесь
OPTIONS = {
    'argv_emulation': True,
    'packages': ['PIL', 'cloudinary', 'woocommerce', 'telethon', 'requests'],
    # 'iconfile': 'icon.icns',  # если у вас есть иконка приложения, добавьте и раскомментируйте
    'bundle_identifier': 'com.yourname.wctgsync'
}

setup(
    app=APP,
    name='wc-tg-sync',
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
