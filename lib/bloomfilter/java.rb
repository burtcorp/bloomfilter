# encoding: utf-8

require 'java'
require 'ext/java-bloomfilter-0.9.3'

module Jar
  import com.skjegstad.utils.BloomFilter
end

module Bloomfilter
  class Java
    def initialize(options = {})
      if options[:size] && options[:false_positive_percentage]
        @filter = Jar::BloomFilter.new(options[:false_positive_percentage], options[:size])
      elsif options[:size] && options[:hashes] && options[:error_probability]
        c = ( Math.log(2) * Math.log2(1.0/options[:error_probability]))
        n = (options[:size].to_f / c).ceil
        k = options[:hashes]
        @filter = Jar::BloomFilter.new(c, n, k)
      elsif options[:filter]
        @filter = options[:filter]
      end
    end
  
    def << (k)
      if include?(k)
        false
      else
        @filter.add(k)
        true
      end
      
    end
    alias :insert :<<
    
    def include?(k)
      @filter.contains(k)
    end
  
    def count
      @filter.count
    end

    def dump
      self
    end
  end
end
