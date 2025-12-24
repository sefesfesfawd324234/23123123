from setuptools import setup

APP = ['main.py']  # <- замените, если ваш entry называется иначе

OPTIONS = {
    'argv_emulation': True,
    'packages': ['PIL', 'cloudinary', 'woocommerce', 'telethon', 'requests'],
    'plist': {
        'CFBundleIdentifier': 'com.yourname.wctgsync'
    }
}

setup(
    app=APP,
    name='wc-tg-sync',
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
