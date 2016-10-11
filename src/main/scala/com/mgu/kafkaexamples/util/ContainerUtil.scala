package com.mgu.kafkaexamples.util

import com.google.common.collect.ImmutableMap
import com.mgu.kafkaexamples.adapter.external.ExternalInvoker

object ContainerUtil {

  private val getHostIpCmd = ExternalInvoker.of("docker inspect")
    .withArgument("--format")
    .withArgument("'{{ .NetworkSettings.Networks.docker_default.IPAddress }}'")
    .withArgument("${containerName}")
    .build

  def getHostIp(containerName: String) = getHostIpCmd
    .invokeWith(ImmutableMap.of("containerName", containerName))
    .replace("\n", "")
    .replace("\"", "")
}
