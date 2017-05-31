# Okra 

Okra implementation using asynchronous MongoDB Java Driver.
This is the fastest asynchronous Okra implementation ever made.

This is a Work In Progress, but will be released very soon!

[![Build Status][travis-badge]][travis-url] [![Code Coverage][codecov-badge]][codecov-url] [![Artifacts][jitpack-badge]][jitpack-url]

## Requirements

* Java 8
* MongoDB Asynchronous Driver

## Note 

Pull Requests are always welcome! We will always review and accept them really fast.

## Dependency

### Gradle

build.gradle

```groovy
    allprojects {
        repositories {
            maven { url "https://jitpack.io" }
        }
    }
```

```groovy
    dependencies {
        compile 'com.github.OkraScheduler:OkraAsync:x.y.z'
    }
```

### Maven

```xml
    <dependency>
        <groupId>com.github.OkraScheduler</groupId>
        <artifactId>OkraAsync</artifactId>
        <version>x.y.z</version>
    </dependency>

    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>
```

[codecov-badge]: https://codecov.io/gh/OkraScheduler/OkraAsync/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/OkraScheduler/OkraAsync
[travis-badge]: https://travis-ci.org/OkraScheduler/OkraAsync.svg?branch=master
[travis-url]: https://travis-ci.org/OkraScheduler/OkraAsync
[jitpack-badge]: https://jitpack.io/v/OkraScheduler/OkraAsync.svg
[jitpack-url]: https://jitpack.io/#OkraScheduler/OkraAsync