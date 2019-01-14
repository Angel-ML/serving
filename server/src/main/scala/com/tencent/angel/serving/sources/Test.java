package com.tencent.angel.serving.sources;

import com.ceph.fs.CephMount;

public class Test {
  public static void main(String[] args){
	CephMount mount = new CephMount();
	System.out.println(mount.conf_get("mon_host"));
  }
}
