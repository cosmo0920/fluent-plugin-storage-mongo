# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-storage-mongo"
  spec.version       = "0.0.4"
  spec.authors       = ["Hiroshi Hatake"]
  spec.email         = ["cosmo0920.oucc@gmail.com"]

  spec.summary       = %q{Fluentd storage plugin for MongoDB.}
  spec.description   = %q{Fluentd storage plugin for MongoDB.}
  spec.homepage      = "https://github.com/cosmo0920/fluent-plugin-storage-mongo"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "https://rubygems.org"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.13"
  spec.add_development_dependency "rake", "~> 12.0"
  spec.add_development_dependency "test-unit", "~> 3.2.0"
  spec.add_dependency "fluentd", [">= 0.14.12", "< 2"]
  spec.add_dependency "mongo", "~> 2.4.0"
end
