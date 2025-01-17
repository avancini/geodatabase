#!/usr/bin/env bash

# ruby, java, git, curl and couchdb
apt-get update
apt-get install aptitude wget curl git tmux vim libxslt-dev libxml2-dev ruby1.9.1-dev libssl-dev build-essential libpq-dev imagemagick libmagickwand-dev -y

# config ruby gems to https
gem sources -r http://rubygems.org
gem sources -r http://rubygems.org/
gem sources -a https://rubygems.org
gem install bundler
gem install pg

