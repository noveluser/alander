#!/usr/bin/python
# coding=utf-8


def aquare_root(x, y):
    z = (y+x/y)/2
    print(z)
    if abs(z - y) < 0.001:
        return z
    else:
        aquare_root(x, z)


if __name__ == "__main__":
    x = 1500
    print(aquare_root(x, 2))
