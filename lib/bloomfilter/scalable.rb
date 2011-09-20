module Bloomfilter
  class Scalable
    attr_reader :count

    def initialize(opts)
      @opts = {
        :initial_size => 100,
        :error_probability_bound => 0.01,
        :error_probability_tightening_factor => 0.5,
        :filter_size_growth_factor => 2,
        :namespace => 'scalable',
        :seed => Time.now.to_i,
        :filter_class => Redis
      }.merge(opts)
      @filters = []
      @count = 0
    end

    def size
      @filters.size
    end
    
    def element_limit(num_bits, error_probability)
      (num_bits * (Math.log(2) ** 2) / Math.log(error_probability).abs).ceil # ** 2
    end

    def hash_count(error_probability)
      ( Math.log2(1/error_probability) ).ceil
    end

    def initial_probability
      @opts[:error_probability_bound] * (1 - @opts[:error_probability_tightening_factor])
    end

    def next_filter_limit
      0 if @count == 0
      filter_limit_sum = 0
      probability = initial_probability
      size = @opts[:initial_size]
      @filters.size.times do |i|
        filter_limit_sum += element_limit(size, probability)
        probability *= @opts[:error_probability_tightening_factor]
        size *= @opts[:filter_size_growth_factor]
      end
      filter_limit_sum
    end

    def next_filter_limit_reached?
      @count >= next_filter_limit
    end

    def create_filter(options)
      @opts[:filter_class].new(options)
    end
    
    def add_filter!
      i = @filters.size
      @filters << create_filter({
                                  :size => @opts[:initial_size] * (@opts[:filter_size_growth_factor] ** i),
                                  :hashes => hash_count(initial_probability * (@opts[:error_probability_tightening_factor] ** i)),
                                  :error_probability => initial_probability * (@opts[:error_probability_tightening_factor] ** i),
                                  :namespace => key_for_index(i),
                                  :seed => @opts[:seed],
                                  :eager => true
                })
    end

    def include?(key)
      @filters.any? { |f| f.include?(key) }
    end

    def keys
      @filters.each_index.map { |i| key_for_index(i) }
    end

    def key_for_index(i)
      "#{@opts[:namespace]}/#{i}"
    end

    def delete_keys!
      @filters.each do |f|
        f.delete_key!
      end
    end
    
    def insert(key)
      if include?(key)
        false
      else
        add_filter! if next_filter_limit_reached?
        @filters.last.insert(key)
        @count += 1
        true
      end
    end
  end
end
