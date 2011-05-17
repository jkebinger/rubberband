require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
describe "bulk ops" do
  before(:all) do
    @index = 'first-' + Time.now.to_i.to_s
    @client = ElasticSearch.new('127.0.0.1:9200', :index => @index, :type => "tweet")
  end

  after(:all) do
    @client.delete_index(@index)
    sleep(1)
  end

  it "should index documents in bulk" do
    @client.bulk do |c|
      c.index({:foo => 'bar'}, :id => '1')
      c.index({:foo => 'baz'}, :id => '2')
    end
    @client.bulk do
      @client.index({:socks => 'stripey'}, :id => '3')
      @client.index({:socks => 'argyle'}, :id => '4')
    end
    @client.refresh

    @client.get("1").foo.should == "bar"
    @client.get("2").foo.should == "baz"
    @client.get("3").socks.should == "stripey"
    @client.get("4").socks.should == "argyle"
  end

  it "should take parent options on bulk operations operations" do
    bulk_data = [
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '1'}}, 
      {:foo => 'tag1'},
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '2', :_parent => '1'}}, 
      {:foo => 'tag2', :_parent => '1'},
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '3', :_parent => '1'}}, 
      {:foo => 'tag3'},
      {:delete => {:_index => @index, :_type => :blog_tag, :_id => '3', :_parent => '1'}}, 
    ]
    @client.expects(:execute).with(:bulk,bulk_data, {})
    @client.bulk do |c|
      c.index({:foo => 'tag1'}, :id => '1', :type => :blog_tag)
      c.index({:foo => 'tag2', :_parent => '1'}, :id => '2', :type => :blog_tag)
      c.index({:foo => 'tag3'}, :id => '3', :type => :blog_tag, :parent => '1')
      c.delete('3', :type => :blog_tag, :parent => '1')
    end

  end

  it "should take routing options for routing on bulk operations" do
    bulk_data = [
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '1'}}, 
      {:foo => 'tag1'},
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '2', :_routing => '1'}}, 
      {:foo => 'tag2', :_routing => '1'},
      {:index => {:_index => @index, :_type => :blog_tag, :_id => '3', :_routing => '1'}}, 
      {:foo => 'tag3'},
      {:delete => {:_index => @index, :_type => :blog_tag, :_id => '3', :_routing => '1'}}, 
    ]
    @client.expects(:execute).with(:bulk,bulk_data, {})
    @client.bulk do |c|
      c.index({:foo => 'tag1'}, :id => '1', :type => :blog_tag)
      c.index({:foo => 'tag2', :_routing => '1'}, :id => '2', :type => :blog_tag)
      c.index({:foo => 'tag3'}, :id => '3', :type => :blog_tag, :routing => '1')
      c.delete('3', :type => :blog_tag, :routing => '1')
    end

  end

end
