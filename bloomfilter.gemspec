# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

Dir['ext/*.jar'].each { |jar| require jar }

Gem::Specification.new do |s|
  s.name        = 'jruby-bloomfilter'
  s.version     = '2.0.0'
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Daniel Gaiottino', 'David Tollmyr', 'Bjorn Ramberg']
  s.email       = ['daniel@burtcorp.com', 'david@burtcorp.com', 'bjorn@burtcorp.com']
  s.homepage    = 'http://github.com/gaiottino/bloomfilter'
  s.summary     = %q{JRuby wrapper for java-bloomfilter}
  s.description = %q{JRuby wrapper (+ some extra functionality) to http://code.google.com/p/java-bloomfilter}

  s.rubyforge_project = 'jruby-bloomfilter'
  s.add_runtime_dependency 'jets3t-rb'
  s.add_runtime_dependency 'redis'

  s.add_development_dependency 'rspec'

  s.files         = `git ls-files`.split("\n")
  #s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  #s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = %w(lib)
end
