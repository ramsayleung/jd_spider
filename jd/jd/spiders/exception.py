#!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# author:Samray <samrayleung@gmail.com>


class ParseNotSupportedError(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return 'url {} is could not be parsed '.format(self.url)
