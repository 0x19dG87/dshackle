package io.emeraldpay.dshackle.upstream.cache

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BlockNotFoundCacheTest {

    @Test
    fun `extract hash from block not found with 0x prefix`() {
        val hash = BlockNotFoundCache.extractBlockHashFromError(
            "block not found: 0xffbc095e062ba35aea23004b9d967c5d29e5136b431df960e71d55fc761a7875",
        )
        assertThat(hash).isEqualTo("0xffbc095e062ba35aea23004b9d967c5d29e5136b431df960e71d55fc761a7875")
    }

    @Test
    fun `extract hash from block hash not found without 0x prefix`() {
        val hash = BlockNotFoundCache.extractBlockHashFromError(
            "block ffbc095e062ba35aea23004b9d967c5d29e5136b431df960e71d55fc761a7875 not found",
        )
        assertThat(hash).isEqualTo("0xffbc095e062ba35aea23004b9d967c5d29e5136b431df960e71d55fc761a7875")
    }

    @Test
    fun `return null for unrelated error`() {
        val hash = BlockNotFoundCache.extractBlockHashFromError("execution reverted: something failed")
        assertThat(hash).isNull()
    }

    @Test
    fun `return null for null message`() {
        val hash = BlockNotFoundCache.extractBlockHashFromError(null)
        assertThat(hash).isNull()
    }

    @Test
    fun `extract hash is lowercase`() {
        val hash = BlockNotFoundCache.extractBlockHashFromError(
            "block FFBC095E062BA35AEA23004B9D967C5D29E5136B431DF960E71D55FC761A7875 not found",
        )
        assertThat(hash).isEqualTo("0xffbc095e062ba35aea23004b9d967c5d29e5136b431df960e71d55fc761a7875")
    }
}
