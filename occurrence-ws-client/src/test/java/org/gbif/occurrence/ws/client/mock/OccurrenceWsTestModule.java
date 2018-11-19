package org.gbif.occurrence.ws.client.mock;


import java.util.List;
import java.util.Properties;

import org.gbif.ws.server.guice.GbifServletListener;

import com.google.common.collect.Lists;
import com.google.inject.Module;

public class OccurrenceWsTestModule extends GbifServletListener {

  public OccurrenceWsTestModule() {
    super("occurrence-test.properties", "org.gbif.occurrence.ws", false);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new OccurrenceWsMockModule());

    return modules;
  }

}
