require 'rubygems'
require 'minitest/unit'

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'yahoofinance-typhoeus'

class MiniTest::Unit::TestCase
end

MiniTest::Unit.autorun
