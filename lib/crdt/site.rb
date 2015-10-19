module CRDT
  # Represents one site (node, client, device, replica, whatever you want to call it).
  # Each site must have a unique +site_id+.
  class Site
    attr_reader :site_id, :vclock

    def initialize(site_id)
      @site_id = site_id
      @vclock = Hash.new(0)
    end

    # Returns +true+ if the causal dependencies of +operation+ have been satisfied,
    # so it is ready to be delivered to this site.
    def causally_ready?(operation)
      (self.vclock.keys | operation.vclock.keys).all? do |site_id|
        if operation.origin == site_id
          operation.vclock[site_id] == self.vclock[site_id] + 1
        else
          operation.vclock[site_id] <= self.vclock[site_id]
        end
      end
    end
  end
end
