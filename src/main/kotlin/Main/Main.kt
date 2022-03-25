import net.jcip.annotations.GuardedBy

import java.util.*
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

fun main() {
    val containerFactory = ContainerFactory()
    val configurations = listOf("a", "b", "c")

    val threads = (1..10).map {
        Thread {
            val container = containerFactory.acquireContainer(configurations[it % configurations.size])
            Thread.sleep(5000) // Pretend that thread is doing some blocking work
            containerFactory.releaseContainer(container.containerId)
        }
    }
    threads.forEach { it.start() }
    threads.forEach { it.join() }
}

typealias Configuration = String
typealias ContainerId = String

class Container(val containerId: ContainerId) {
    private val stoppedTimes = LongAdder()

    fun run() {
        if (stoppedTimes.sum() > 0) throw IllegalStateException("Running previously stopped container")
        println("Container: $containerId runs on thread: ${Thread.currentThread().name}")
    }

    fun stop() {
        stoppedTimes.increment()
        Thread.sleep(2000)
    }
}

class ContainerFactory(val maxContainers: Int = 2) {

    private val registryGuard: Lock = ReentrantLock(true)
    private val containerAddedOrReleased: Condition = registryGuard.newCondition()

    @GuardedBy("registryGuard")
    val containerRegistry: MutableMap<Configuration, Container> = HashMap()

    @GuardedBy("registryGuard")
    val recentlyReleasedContainers: MutableSet<ContainerId> = HashSet()

    @GuardedBy("registryGuard")
    val activeContainers: MutableMap<ContainerId, LongAdder> = HashMap()

    fun acquireContainer(configuration: Configuration): Container = guardWithLock {
        when {
            containerRegistry.containsKey(configuration) -> getExistingContainer(configuration)
            containerRegistry.isNotFull() -> createContainer(configuration)
            else -> awaitThenCreateOrGetExistingContainer(configuration)
        }.also { it.run() }
    }

    private fun getExistingContainer(configuration: Configuration): Container =
        containerRegistry[configuration]!!.also {
            registerAsActive(it.containerId)
            println(
                "Thread ${Thread.currentThread().name}: Acquiring existing container ${it.containerId} for " +
                    "configuration $configuration"
            )
        }

    private fun createContainer(configuration: Configuration): Container =
        Container(UUID.randomUUID().toString()).also {
            containerRegistry[configuration] = it
            containerAddedOrReleased.signalAll()
            registerAsActive(it.containerId)
            println(
                "Thread ${Thread.currentThread().name}: Creating new container ${it.containerId} for configuration " +
                    configuration
            )
        }

    private fun registerAsActive(containerId: ContainerId) {
        activeContainers.computeIfAbsent(containerId) {
            LongAdder()
        }.increment()
        recentlyReleasedContainers.remove(containerId)
    }

    private fun awaitThenCreateOrGetExistingContainer(configuration: Configuration): Container {
        while (
            !containerRegistry.containsKey(configuration)
            && recentlyReleasedContainers.isEmpty()
        ) {
            println(
                "Thread ${Thread.currentThread().name}: woken up on condition, value: ${
                    !containerRegistry.containsKey(configuration)
                        && recentlyReleasedContainers.isEmpty()
                } "
            )
            containerAddedOrReleased.await()
        }

        return when {
            containerRegistry.containsKey(configuration) -> // check if needed container has been added by another thread
                getExistingContainer(configuration)

            recentlyReleasedContainers.isNotEmpty() -> { // check we have a released container and we can remove it
                stopAndRemoveContainer(recentlyReleasedContainers.random())
                createContainer(configuration)
            }

            else -> throw IllegalStateException("Should never happen")
        }
    }

    private fun stopAndRemoveContainer(containerId: ContainerId) {
        recentlyReleasedContainers.remove(containerId)
        containerRegistry.remove(containerId)?.stop().also {
            println("Thread ${Thread.currentThread().name}: Stopping and removing container $containerId")
        }
    }

    fun releaseContainer(containerId: ContainerId) = guardWithLock {
        // If containerId is invalid
        if (!containerRegistry.values.any { it.containerId == containerId }
            && !activeContainers.containsKey(containerId)) {
            throw IllegalArgumentException("Invalid containerId: $containerId passed to be released")
        }

        val activeContainersAdder = activeContainers[containerId]!!
        activeContainersAdder.decrement()
        println(
            "Thread ${Thread.currentThread().name}: Releasing container $containerId, another users count " +
                "${activeContainersAdder.sum()}"
        )

        if (activeContainersAdder.sum() == 0L) {
            activeContainers.remove(containerId)
            recentlyReleasedContainers.add(containerId)
            containerAddedOrReleased.signalAll()
        }
    }

    private fun <T> guardWithLock(block: () -> T) =
        try {
            registryGuard.lock()
            block()
        } finally {
            registryGuard.unlock()
        }

    private fun Map<Configuration, Container>.isNotFull(): Boolean = size < maxContainers
}
