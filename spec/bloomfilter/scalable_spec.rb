require 'spec_helper'
require 'benchmark'

module Bloomfilter
  describe Scalable do
    def clear_redis!
      redis = ::Redis.new
      redis.keys('*scalable_spec*').each do |k|
        redis.del k
      end
    end

    [Redis, Java].each do |klass|
      context "using #{klass}" do

        context 'weighted count' do
          before do
            clear_redis!
          end
          after do
            clear_redis!
          end
          it 'should not define @weighted_count unless version >=2' do
            Scalable.new(:filter_class => klass, :version => nil)
              .instance_variables.should_not include(:@weighted_count)
            Scalable.new(:filter_class => klass, :version => 2)
              .instance_variables.should include(:@weighted_count)
          end
          it 'updates weighted count correctly' do
            scalable = Scalable.new(
              :namespace => 'scalable_spec',
              :seed => 1447271,
              :filter_class => klass, 
              :version => 2)
            100.times do |i|
              scalable.insert("key#{i}",i)
            end
            scalable.count.should be_between(99,100)
            scalable.weighted_count.should be_between(4455,4950)
          end
        end

        context 'with little data' do
          before do
            clear_redis!
            @opts = {
              :initial_size => 100,
              :error_probability_bound => 0.01,
              :error_probability_tightening_factor => 0.5,
              :filter_size_growth_factor => 2,
              :namespace => 'scalable_spec',
              :seed => 1447271,
              :filter_class => klass
            }
            @scalable = Scalable.new(@opts)
          end

          after do
            # clear_redis!
          end

          it 'starts out with no filters' do
            @scalable.size.should == 0
          end

          it "initializes #{klass} filter with correct options when creating first filter" do
            klass.should_receive(:new).with({
                                              :size => @opts[:initial_size],
                                              :hashes => Math.log2(1 / (@opts[:error_probability_bound] * (1 - @opts[:error_probability_tightening_factor])) ).ceil,
                                              :error_probability=>0.005,
                                              :namespace => "scalable_spec/0",
                                              :seed => @opts[:seed],
                                              :eager => true
                                            }).and_return(double('redis').as_null_object)
            @scalable.insert('apa')
          end

          it 'has one filter when one key has been added' do
            @scalable.insert('apa')
            @scalable.size.should == 1
          end

          it 'has one filter when two keys have been added' do
            @scalable.insert('apa')
            @scalable.insert('bapa')
            @scalable.size.should == 1
          end
          
          context 'when the first filter is full' do
            before do
              @scalable = Scalable.new(@opts.merge({ :initial_size => 100 }))
            end
            
            it 'adds a new filter' do
              20.times { |i| @scalable.insert("key#{i}") }
              @scalable.size.should == 2
            end

            it "instantiates #{klass} with correct options" do
              bloomfilter = double('bloomfilter').as_null_object
              bloomfilter.stub(:include?).and_return(false)
              klass.should_receive(:new).once.ordered.and_return(bloomfilter)
              klass.should_receive(:new).once.ordered do |opts|
                opts.should == {
                  :size => @opts[:initial_size] * 2,
                  :hashes => Math.log2(1 / (@opts[:error_probability_bound] * (1 - @opts[:error_probability_tightening_factor]) * @opts[:error_probability_tightening_factor]) ).ceil,
                  :error_probability=>0.0025,
                  :namespace => "scalable_spec/1",
                  :seed => @opts[:seed],
                  :eager => true
                }
                r = double('bloomfilter2').as_null_object
                r.stub(:include?).and_return(false)
                r
              end
              
              15.times { |i| @scalable.insert("skey#{i}") }
              @scalable.size.should == 2
              @scalable.count.should == 15
            end

            it 'does not add to the second filter if an item is in the first filter' do
              bloomfilter = double('bloomfilter').as_null_object
              bloomfilter.stub(:include?) do |i|
                i >= 11 # Effect: all items >= 11 are already included in the first filter.
              end
              klass.should_receive(:new).once.ordered.and_return(bloomfilter)
              second_bloomfilter = double('bloomfilter').as_null_object
              second_bloomfilter.should_receive(:insert).exactly(1).times
              klass.should_receive(:new).once.ordered.and_return(second_bloomfilter)
              15.times { |i| @scalable.insert(i) }
              @scalable.count.should == 11
            end
          end

          context '#include?' do
            it "checks the first filter" do
              15.times { |i| @scalable.insert(i) }
              @scalable.include?(5).should be_true
            end
            it "checks the last filter" do
              15.times { |i| @scalable.insert(i) }
              @scalable.include?(14).should be_true
            end
          end

          context '#keys' do
            it 'gives all redis keys' do
              100.times do |i|
                @scalable.insert((i*1000).to_s(36))
              end
              @scalable.keys.should == ['scalable_spec/0', 'scalable_spec/1', 'scalable_spec/2', 'scalable_spec/3']
            end
          end

          context '#delete_keys!' do
            it 'deletes all redis keys' do
              r = ::Redis.new
              100.times do |i|
                @scalable.insert((i*1000).to_s(36))
              end
              r.keys('*scalable_spec*').sort.should == ['scalable_spec/0', 'scalable_spec/1', 'scalable_spec/2', 'scalable_spec/3'].sort if klass == Redis
              @scalable.delete_keys!
              r.keys('*scalable_spec*').should == []
            end
          end

          context '#key_for_index' do
            it 'gives a namespaced key' do
              @scalable.key_for_index(5).should == 'scalable_spec/5'
            end
          end

        end
        
        context 'with lots of duplicate data' do
          before :all do
            clear_redis!
            @opts = {
              :initial_size => 100,
              :error_probability_bound => 0.01,
              :error_probability_tightening_factor => 0.5,
              :filter_size_growth_factor => 2,
              :namespace => 'scalable_spec',
              :seed => 1447271,
              :filter_class => klass
            }

            @data = 20000.times.map do |i|
              Zlib.crc32("key#{i}").to_s
            end
            @insert_count = 0
            puts "Inserting #{@data.size} keys, timed: "
            @new_scalable = nil
            puts Benchmark.measure { 
              @new_scalable = Scalable.new(@opts.merge({ :initial_size => 1000000 }))
              2.times do
                @data.each do |k|
                  @insert_count += @new_scalable.insert(k) ? 1 : 0
                end
              end
            }
          end

          after :all do
            # clear_redis!
          end

          it 'keeps the count' do
            @new_scalable.count.should be_between(0.99*20000, 20000)
          end
          
          it 'respects the false positive bound' do
            false_positive_percentage = (@data.size.to_f - @insert_count)/@data.size
            false_positive_percentage.should be_between(0, 0.01)
          end

          it 'includes all keys' do
            @data.each do |k|
              @new_scalable.include?(k).should be_true
            end
          end

          it 'does not include too many made up keys' do
            not_included_count = 0
            not_included = (20001..30000).map do |i|
              k = Zlib.crc32("key#{i}").to_s
              not_included_count += @new_scalable.include?(k) ? 0 : 1
            end
            not_included_count.should be_between(0.99*10000, 10000)
          end
        end
      end
    end

    context 'serialization' do

      before :all do
        @opts = {
          :initial_size => 20000,
          :error_probability_bound => 0.01,
          :error_probability_tightening_factor => 0.5,
          :filter_size_growth_factor => 2,
          :namespace => 'scalable',
          :seed => Time.now.to_i,
          :filter_class => Bloomfilter::Java
        }
      end

      it 'should deserialize objects of version <2 properly' do
        scalable = Scalable.new(@opts)
        scalable.send(:remove_instance_variable, :@weighted_count)
        scalable.instance_variable_get(:@opts).delete(:version)
        100.times do |i|
          scalable.insert(i.to_s)
        end
        str = Marshal.dump(scalable)
        scalable = Marshal.load(str)
        scalable.instance_variables.should_not include :@weighted_count
        scalable.count.should be_between(99,100)
        scalable.version.should == 1
      end

      it 'should deserialize objects of version 2 properly' do
        scalable = Scalable.new(@opts.merge(:version => 2))
        100.times do |i|
          scalable.insert(i.to_s,10)
        end
        str = Marshal.dump(scalable)
        scalable = Marshal.load(str)
        scalable.count.should be_between(99,100)
        scalable.weighted_count.should be_between(990,1000)
        scalable.version.should == 2
      end
    end
  
  end
end
