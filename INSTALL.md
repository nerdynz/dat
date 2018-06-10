# Installation

Go package management is a mess right now. gopkg.in was a band-aid and 
worked well if dependencies were on the master branch or followed gopkg.in
convention. The problem is and has always been how do you lock down to 
a specific commit or tag? That's where glide shines.

## Pre-requisites

Install [glide](https://github.com/Masterminds/glide)

## Installing to $GOPATH

If you are not using glide in your project

```sh
cd $GOPATH/src
# remove dir if it exists
# rm -rf github.com/nerdynz/dat
git clone -b v1 https://github.com/mgutz/dat github.com/nerdynz/dat
cd github.com/nerdynz/dat
glide install
```

## Existing Glide Project

If you are already using glide for your project

```sh
glide get github.com/nerdynz/dat
```
