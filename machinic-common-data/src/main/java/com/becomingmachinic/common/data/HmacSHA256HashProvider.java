package com.becomingmachinic.common.data;

/**
 * The HMAC SHA256 provider hashes the data using the SHA256 HMAC (Keyed hash) to make it impossible for an attacker to predict a 
 * resulting and therefore exploit a collision in the bloom filter. For this to be true the hashKey must remain unknown to the attacker. 
 * A HMAC SHA256 has is more computationally expensive than a standard SHA256 hash so if this added security is not needed then the standard 
 * SHA256 provider should be used.
 * 
 * @author caleb
 *
 */
public class HmacSHA256HashProvider implements HashStreamProvider{

	private final byte[] hashKey;
	
	/**
	 * While any key size will work the recommended key size is 32 bytes.
	 * @param hashKey
	 */
	public HmacSHA256HashProvider(final byte[] hashKey) {
		this.hashKey = hashKey;
	}
	
	@Override
	public HashStream createHashStream() throws HashStreamException {
		return new HmacSHA256HashStream(hashKey);
	}
	
}
