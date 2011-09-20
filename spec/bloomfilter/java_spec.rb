# encoding: utf-8

require 'spec_helper'

module Bloomfilter
  describe Java do
    before :each do
      @filter = Java.new(:size => 1_000, :false_positive_percentage => 0.01)
    end
  
    it 'should not have any elements from start' do
      @filter.count.should == 0
    end

    [:<<, :insert].each do |m|
      describe "##{m}" do
        it 'should be possible to add elements which updates the count' do
          @filter.send(m, 'hello')
          @filter.count.should == 1
        end

        it 'should return true if an element is added' do
          @filter.send(m, 'hello').should == true
        end

        it 'should return false if an element is not added' do
          @filter.send(m, 'hello')
          @filter.send(m, 'hello').should == false
        end
      end
    end
  
    describe '#include?' do
      it 'should return false if an element does not exist' do
        @filter.include?('world').should be_false
      end
    
      it 'should return true if an element exists' do
        @filter.include?('hello').should be_false
      
        @filter << 'world'
        @filter.include?('world').should be_true
      end
    end
  end
end
