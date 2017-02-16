# Mongo Storage plugin for Fluent

[![Build Status](https://travis-ci.org/cosmo0920/fluent-plugin-storage-mongo.svg?branch=master)](https://travis-ci.org/cosmo0920/fluent-plugin-storage-mongo)

fluent-plugin-storage-mongo is a fluentd plugin to store plugin state into mongodb.

## Prerequisite

* MongoDB 3.0 or later.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'fluent-plugin-storage-mongo'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-storage-mongo

## Configuration

Use _mongo_ type in store.

```aconf
<store>
  @type mongo
  database fluent
  collection test

  # Following attibutes are optional
  host fluenter
  port 10000

  # capped option is not implemented yet.

  # Set 'user' and 'password' for authentication
  user handa
  password shinobu

  # Other buffer configurations here
</store>
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/cosmo0920/fluent-plugin-storage-mongo.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
