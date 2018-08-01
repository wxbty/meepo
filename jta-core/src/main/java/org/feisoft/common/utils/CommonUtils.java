
package org.feisoft.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class CommonUtils {
	static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

	public static boolean instanceEquals(String source, String target) {
		if (StringUtils.isBlank(source) && StringUtils.isNotBlank(target)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isNotBlank(source)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isBlank(source)) {
			return true;
		} else {
			String[] sourceArray = source.split("\\s*:\\s*");
			String[] targetArray = target.split("\\s*:\\s*");
			if (sourceArray.length != targetArray.length) {
				return false;
			} else {
				String sourceAddr = sourceArray[0];
				String targetAddr = targetArray[0];
				String sourcePort = sourceArray[sourceArray.length - 1];
				String targetPort = targetArray[targetArray.length - 1];
				return StringUtils.equalsIgnoreCase(sourceAddr, targetAddr)
						&& StringUtils.equalsIgnoreCase(sourcePort, targetPort);
			}
		}
	}

	public static boolean equals(Object o1, Object o2) {
		if (o1 != null) {
			return o1.equals(o2);
		} else if (o2 != null) {
			return o2.equals(o1);
		} else {
			return true;
		}
	}

	@Deprecated
	public static byte[] serializeObject(Serializable object) throws IOException {
		return SerializeUtils.serializeObject(object);
	}

	@Deprecated
	public static Serializable deserializeObject(byte[] byteArray) throws IOException {
		try {
			return SerializeUtils.deserializeObject(byteArray);
		} catch (Exception ex) {
			// Hessian was the only serializer before 0.4.1
			return SerializeUtils.hessianDeserialize(byteArray);
		}
	}

	public static void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	public static String getInetAddress() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			while (enumeration.hasMoreElements()) {
				NetworkInterface ni = enumeration.nextElement();
				if (ni.isLoopback()) {
					continue;
				} else if (ni.isUp() == false) {
					continue;
				}

				Enumeration<InetAddress> inetAddrList = ni.getInetAddresses();
				while (inetAddrList.hasMoreElements()) {
					InetAddress inetAddr = inetAddrList.nextElement();

					if (inetAddr.isLoopbackAddress()) {
						continue;
					} else if (inetAddr.isMulticastAddress()) {
						continue;
					} else if (inetAddr.isAnyLocalAddress()) {
						continue;
					} else if (Inet4Address.class.isInstance(inetAddr) == false) {
						continue;
					}

					return inetAddr.getHostAddress();
				}

			}

			InetAddress inetAddr = InetAddress.getLocalHost();
			return inetAddr.getHostAddress();
		} catch (Exception ex) {
			logger.error("Error occurred while getting ip address.", ex);
			return "127.0.0.1";
		}
	}

	public static String getInetAddress(String host) {
		try {
			InetAddress inetAddr = InetAddress.getByName(host);
			return inetAddr.getHostAddress();
		} catch (UnknownHostException ex) {
			logger.error("Error occurred while getting ip address: host= {}.", host, ex);
			return host;
		}
	}

}
