import logging
import argparse

class Handler:
    def handle(self, args):
        logging.info("%s handling %s", type(self), args)
        p = argparse.ArgumentParser("handler")
        p.add_argument("action", choices=["download"])
        opts = p.parse_args(args)

        if opts.action == "download":
            self.download()

class Topics(Handler):
    def __init__(self, content):
        self.content = content

    def download(self):
        print()

    