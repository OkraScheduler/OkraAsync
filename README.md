# OkraCore - Okra Asynchronous Time Based Queue 

Okra implementation using asynchronous MongoDB Java Driver.
This is the fastest asynchronous Okra implementation ever made.

This is a Work In Progress, but will be released very soon!

[![codecov](https://codecov.io/gh/OkraScheduler/OkraCore/branch/master/graph/badge.svg)](https://codecov.io/gh/OkraScheduler/OkraCore)

[![Build Status](https://travis-ci.org/OkraScheduler/OkraCore.svg?branch=master)](https://travis-ci.org/OkraScheduler/OkraCore)

### Requirements

* Java 8
* MongoDB Asynchronous Driver

### Note 

Pull Requests are always welcome! We will always review and accept them really fast.

### Binaries

[![](https://jitpack.io/v/OkraScheduler/OkraCore.svg)](https://jitpack.io/#OkraScheduler/OkraCore)

#### Gradle
build.gradle
```groovy
    allprojects {
        repositories {
            ...
            maven { url "https://jitpack.io" }
        }
    }
```

```groovy
    dependencies {
        compile 'com.github.OkraScheduler:OkraCore:x.y.z'
    }
```

#### Maven
```xml
	<dependency>
	    <groupId>com.github.OkraScheduler</groupId>
	    <artifactId>OkraCore</artifactId>
	    <version>x.y.z</version>
	</dependency>

	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
```