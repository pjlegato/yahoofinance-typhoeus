#
# High-performance historic stock data download from Yahoo! Finance.
# Uses the Typhoeus library and libcurl.
#
# This project is not endorsed by or connected with Yahoo! in any way.
#
# Author:: Paul Legato (pjlegato at gmail dot com)
# Copyright:: Copyright (C) 2010 Paul Legato. All rights reserved.
# License:: BSD-new license: see the file LICENSE for licensing details.
#

require 'rubygems'
require 'typhoeus'
require 'date'


class YahooFinanceException < Exception
end

class SymbolNotFoundException < YahooFinanceException
end

class YahooFinance

  # Create a new YahooFinance object with the specified maximum number
  # of concurrent connections. The default is 20.
  #
  # If you will be repeating the same query more than once, you can
  # cache the responses from Yahoo by setting memoize_requests to
  # true.
  def initialize(concurrent_connections=20, memoize_requests=false)
    @hydra = Typhoeus::Hydra.new(:max_concurrency => concurrent_connections)
    @hydra.disable_memoization unless memoize_requests
  end

  # Run any pending historic data queries in the queue and potentially
  # execute any defined callbacks.
  # Blocking: will not return until the entire queue has run.
  def run
    @hydra.run
  end


  # Adds a query for the given symbol over the given date range to the
  # queue. The given block is used as a callback that will be called
  # with 1 argument, the response, upon successful completion. Note that no
  # queries will actually execute until #run is called.
  #
  # The dates can be either Date objects or strings. If they're
  # strings, they will be fed to Date.parse.
  #
  # Raises a SymbolNotFoundException if querying that symbol produces a 404 error on Yahoo.
  # Raises a YahooFinanceException for any other problems.
  #
  # Otherwise, your block will be called with the body of the
  # response, the raw data from Yahoo!, as its argument.
  #
  #
  def add_query(symbol, start_date, end_date, &callback)
    start_date = Date.parse(start_date) unless start_date.is_a?(Date)
    end_date = Date.parse(end_date) unless end_date.is_a?(Date)

    @hydra.queue(make_request(symbol, start_date, end_date, callback))

    true
  end


  # Convenience method for one-off quick queries of a given symbol and
  # date range.
  #
  # Note that as this is intended for one-off runs, it is not as
  # efficient as queueing them up yourself and running them in
  # parallel.
  #
  # Returns the raw CSV response data from Yahoo.
  #
  def self.quick_query(symbol, start_date, end_date)
    yf = self.new
    result = nil
    yf.add_query(symbol, start_date, end_date) {|response| result = response }
    yf.run
    result
  end

  private

  # Returns a Typhoeus HTTP request object that will query Yahoo!
  # Finance for the given symbol and date range and call the given
  # callback if all is well.
  def make_request(symbol, start_date, end_date, callback)
    url = "http://itable.finance.yahoo.com" +
      "/table.csv?s=#{ symbol }&g=d" +
      "&a=#{ start_date.month - 1 }&b=#{ start_date.mday }&c=#{ start_date.year }" +
      "&d=#{ end_date.month - 1 }&e=#{ end_date.mday }&f=#{ end_date.year.to_s }"

    request = Typhoeus::Request.new(url, :method => :get)

    request.on_complete {|response|
      if response.code == 200
        if response.body[0..40] != "Date,Open,High,Low,Close,Volume,Adj Close"
          raise YahooFinanceException.new(" * Error: Unknown response body from Yahoo - #{ response.body[0..40] } ...")
        else
          # good response. go.
          callback.call(response.body)
        end
      elsif response.code == 404
        raise SymbolNotFoundException.new(symbol + " not found at Yahoo")
      else
        raise YahooFinanceException.new("Error communicating with Yahoo. Response code #{ response.code }. URL: " +
                                        "#{ url }. Response: #{ response.inspect }")
      end
    }

    request
  end # make_request

end # class
