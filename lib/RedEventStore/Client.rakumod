use Nats;
use JSON::Fast;

unit class RedEventStore::Client;

has Nats $.nats .= new;

method TWEAK(|) { await $!nats.start }

method add-event($event) {
	my $type = $event.^name;
	my %data = $event.^attributes.grep(*.has_accessor).map: -> $attr {
		do with $attr.get_value: $event {
			$attr.name.substr(2) => $_
		}
	}

	my @types = (|$event.^mro, |$event.^roles).map( *.^name ).grep: { $_ ne <Any Mu>.any };

	my \answer = await $!nats.request: "add_event", %( :@types, :%data ).&to-json;
	.return with answer.?json;
	answer
}

method get-events(Int $index = 0, :@types, Instant :$from-timestamp, Instant :$to-timestamp, *%pars) {
	my @events = .json given await $!nats.request:
		[ "get_events", $index ].join("."),
		%(
			|(:@types if @types),
			|(:$from-timestamp with $from-timestamp),
			|(:$to-timestamp with $to-timestamp),
			|%pars
		).&to-json
	;

	gather for @events -> (Str :$type, UInt :$seq, :%data) {
		my $t = ::($type);
		if !$t && $t ~~ Failure {
			require ::($type);
			$t = ::($type);
		}
		$*SOURCING-MESSAGE-SEQ = $seq if $*SOURCING-MESSAGE-SEQ !~~ Failure;
		take $t.new: |%data
	}
}
